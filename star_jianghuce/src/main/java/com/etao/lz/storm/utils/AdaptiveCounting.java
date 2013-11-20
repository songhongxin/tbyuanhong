package com.etao.lz.storm.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.clearspring.analytics.hash.Lookup3Hash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;

/**
 * 基于 AdaptiveCounting 算法实现了位图稀疏存储以节省内存开销，兼容原有的位图格式
 * 
 * 稀疏存储的位图格式为：
 * 
 * <pre>
 * [ ID:1B ] [ L0:1B ] [ IDX0 ] [ L1:1B ] [ IDX1 ] ...
 * </pre>
 * 
 * 其中 <code>IDX*</code> 为有值的分桶下标，长度为 <code>ceil(log2(m)/8)</code>，多于 1 字节时以
 * little-endian 字节序保存； <code>L*</code> 为对应分桶的值。这里只记录非空桶，故 <code>L*</code>
 * 取值总是大于 0。
 * 
 * 由于分桶的值目前不会超过 64（对应最大 2^64-1 的唯一计数），为了同原有位图进行区分，稀疏存储时首字节 <code>ID</code> 值为
 * 128+k（k 为 log2(m)）。
 * 
 * 当桶位图全空时，稀疏存储结构仅保留 <code>ID</code>。
 * 
 * @author wxz
 * 
 */
public class AdaptiveCounting implements ICardinality {

	/**
	 * ((gamma(-(m.^(-1))).* ( (1-2.^(m.^(-1)))./log(2) )).^(-m)).*m
	 */
	static final double[] mAlpha = { 0, 0.44567926005415, 1.2480639342271,
			2.8391255240079, 6.0165231584811, 12.369319965552, 25.073991603109,
			50.482891762521, 101.30047482549, 202.93553337953, 406.20559693552,
			812.74569741657, 1625.8258887309, 3251.9862249084, 6504.3071471860,
			13008.949929672, 26018.222470181, 52036.684135280, 104073.41696276,
			208139.24771523, 416265.57100022, 832478.53851627, 1669443.2499579,
			3356902.8702907, 6863377.8429508, 11978069.823687, 31333767.455026,
			52114301.457757, 72080129.928986, 68945006.880409, 31538957.552704,
			3299942.4347441 };
	static final double B_s = 0.051; // 空桶占比阈值，空桶比例超过该值时即转换为线性估算法

	final int k; // log2(桶位图长度)
	final int sparseIdxLen; // 稀疏存储位图时桶下标占用字节数

	int m; // 完整桶位图长度（包括未使用的桶）
	double Ca; // 基数估算结果渐进修正系数
	byte[] M; // 桶位图
	int Rsum; // 桶计数总和，LogLog 估算法中用于快速计算最大全 0 前缀长度的平均值
	int b_e; // 当前空桶数，用于决定何时进行线性估算法和 LogLog 估算法的切换，以及在线性估算法中估算基数

	@SuppressWarnings("serial")
	static class AdaptiveMergeException extends CardinalityMergeException {
		public AdaptiveMergeException(String message) {
			super(message);
		}
	}

	public AdaptiveCounting(int k) {
		this(k, false);
	}

	public AdaptiveCounting(int nk, boolean startSparse) {
		if (nk >= mAlpha.length) {
			throw new IllegalArgumentException(String.format(
					"Max k (%d) exceeded: k=%d", mAlpha.length - 1, nk));
		}

		k = nk;
		sparseIdxLen = calcBktIdxBytes(nk);

		m = 1 << nk;
		Ca = mAlpha[nk];

		if (startSparse) {
			// 创建稀疏存储位图
			M = new byte[sparseIdxLen + 2]; // 至少保留 ID 字节和 1 个稀疏桶
			M[0] = genSparseId(k);
		} else {
			// 创建正常存储位图
			M = new byte[m];
		}
		b_e = m;
	}

	/**
	 * 从给定位图创建基数估计对象
	 * 
	 * 注意：新建对象会使用给定的位图的副本，副本长度舍入至不小于其长度的最近的 2 的幂次
	 * 
	 * @param extM
	 */
	public AdaptiveCounting(byte[] extM) {
		if (extM.length < 1) {
			throw new IllegalArgumentException(
					String.format(
							"Invalid array size: M.length must be greater than zero, which is %d",
							extM.length));
		}

		boolean hasId = ((extM[0] & 0x80) != 0);
		if (hasId) {
			// 首字节为 ID，是稀疏存储位图
			// 由 ID 推出 k 和 m
			k = kFromSparseId(extM[0]);
			m = 1 << k;
		} else {
			// 首字节非 ID，是正常存储位图
			// 由给定位图长度推出 k 和 m
			m = extM.length;
			k = Integer.numberOfTrailingZeros(m);
			if (m != (1 << k)) {
				throw new IllegalArgumentException(
						String.format(
								"Invalid array size: M.length must be a power of 2, which is %d",
								extM.length));
			}
		}
		// 创建新的内部位图副本，长度舍入到 >=length 的最小 2 的幂次
		int nextPow2 = (int) Math.pow(2,
				Math.ceil(Math.log(extM.length) / Math.log(2)));
		M = Arrays.copyOf(extM, nextPow2);

		if (k >= mAlpha.length) {
			throw new IllegalArgumentException(String.format(
					"Max k (%d) exceeded: k=%d", mAlpha.length - 1, k));
		}
		Ca = mAlpha[k];
		sparseIdxLen = calcBktIdxBytes(k);

		if (hasId) {
			// 计算稀疏桶计数总和及空桶数
			b_e = m - (extM.length - 1) / (sparseIdxLen + 1);
			for (int i = 1; i < extM.length; i += sparseIdxLen + 1) {
				Rsum += extM[i];
			}
		} else {
			// 计算正常桶计数总和及空桶数
			b_e = m;
			for (byte b : extM) {
				if (b > 0) {
					Rsum += b;
					b_e--;
				}
			}
		}
	}

	@Override
	public boolean offer(Object o) {
		// XXX: 此处为了保证兼容之前通过 stream-lib 计算得到的位图，沿用了 lookup3 哈希算法，但实际上用
		// murmurhash 算法会让估算精度略有提升
		long x = Lookup3Hash.lookup3ycs64(o.toString());
		int j = (int) (x >>> (Long.SIZE - k));
		byte r = (byte) (Long.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1);
		boolean modified = false;

		if (isSparse()) {
			// 以稀疏存储形式更新桶计数

			// 查找本次要更新的桶
			int off = sparseBisectSearch(j);
			if (off != -1) {
				// 待更新的桶已存在
				if (M[off] < r) {
					Rsum += r - M[off];
					M[off] = r;
					modified = true;
				}
				return modified;
			}

			// 待更新的桶尚不存在，根据已用桶数量决定采用何种位图存储方式
			if (!shouldUseNormalBmp(m - b_e)) {
				// 沿用稀疏存储格式插入新的桶及关联值
				sparseInsertBucket(j, r);
				// 根据新桶关联值更新计数状态
				Rsum += r;
				b_e--;
				return true;
			}

			// 将稀疏存储格式转换为普通格式
			convertSparseToNormalBmp();
			// to CONT: 沿用普通格式算法更新桶计数
		}

		// CONT: 以普通存储形式更新桶计数
		if (M[j] < r) {
			Rsum += r - M[j];
			if (M[j] == 0)
				b_e--;
			M[j] = r;
			modified = true;
		}

		return modified;
	}

	@Override
	public long cardinality() {
		double B = (b_e / (double) m);

		// 空桶比例高于阈值，使用线性估算法
		if (B >= B_s) {
			return Math.round(-m * Math.log(B));
		}

		// 空桶比例低于阈值，使用LogLog估算法
		double Ravg = Rsum / (double) m;
		return (long) (Ca * Math.pow(2, Ravg));
	}

	@Override
	public int sizeof() {
		return m;
	}

	/**
	 * 输出内部位图，对于普通位图直接返回引用，对于稀疏位图则返回实际使用部分的副本
	 */
	@Override
	public byte[] getBytes() {
		if (isSparse()) {
			// 稀疏位图仅输出实际使用的部分
			return Arrays.copyOf(M, usedLen());
		} else {
			return M;
		}
	}

	@Override
	public ICardinality merge(ICardinality... estimators)
			throws AdaptiveMergeException {
		AdaptiveCounting mergedAc = null;
		byte[] mergedBytes = null;

		// 对待合并基数估计器进行分类
		List<AdaptiveCounting> normEsts = new ArrayList<AdaptiveCounting>();
		List<AdaptiveCounting> sparseEsts = new ArrayList<AdaptiveCounting>();
		classifyBmp(estimators, normEsts, sparseEsts);

		/**
		 * 合并时需考虑以下情况：
		 * <ul>
		 * <li>仅有普通存储格式或稀疏存储和普通存储格式同时出现时，结果使用普通存储格式</li>
		 * <li>仅有稀疏存储格式时，通过多路归并方式保序合并稀疏存储结构，若合并过程中发现改用普通存储格式更省空间，则结果使用普通存储格式</li>
		 * </ul>
		 */
		try {
			if (normEsts.size() > 0) {
				// 存在普通存储格式，结果使用普通存储格式
				mergedBytes = mergeBmpGenNorm(normEsts, sparseEsts);
			} else if (sparseEsts.size() > 0) {
				// 仅存在稀疏存储格式
				int bkts = sparseCountDistBkts(sparseEsts);

				if (shouldUseNormalBmp(bkts)) {
					// 合并后结果用普通存储格式更省内存，将所有稀疏位图合并至空的普通位图
					mergedBytes = mergeBmpGenNorm(null, sparseEsts);
				} else {
					// 合并后结果用稀疏存储格式更省内存
					mergedBytes = mergeBmpGenSparse(bkts, sparseEsts);
				}
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}

		if (mergedBytes != null) {
			mergedAc = new AdaptiveCounting(mergedBytes);
		}
		return mergedAc;
	}

	/**
	 * 计算给定稀疏存储格式估计器中的唯一桶数量
	 * 
	 * @param sparseEsts
	 * @return
	 */
	final int sparseCountDistBkts(List<AdaptiveCounting> sparseEsts) {
		// 包括当前对象在内的位图遍历游标及最大长度数据
		int estNo = sparseEsts.size();
		int[] offs = new int[estNo];
		byte[][] bmps = new byte[estNo][];
		int[] curLens = new int[estNo];

		for (int i = 0; i < estNo; i++) {
			AdaptiveCounting ac = sparseEsts.get(i);
			byte[] sm = ac.M;
			bmps[i] = sm;
			offs[i] = 1;
			curLens[i] = ac.usedLen();
		}

		int step = sparseIdxLen + 1;
		int idx;
		int totalBuckets = 0;
		int minIdx;
		int minEstNo = 0;
		int lastIdx = -1;

		// 统计结果的总桶数
		while (minEstNo != -1) {
			minIdx = Integer.MAX_VALUE;
			minEstNo = -1;
			for (int i = 0; i < estNo; i++) {
				if (offs[i] < curLens[i]) {
					idx = bytesToInt(bmps[i], offs[i] + 1, sparseIdxLen);
					if (idx < minIdx) {
						minIdx = idx;
						minEstNo = i;
					}
				}
			}

			if (minEstNo != -1) {
				offs[minEstNo] += step;
				if (lastIdx != minIdx) {
					totalBuckets++;
					lastIdx = minIdx;
				}
			}
		}

		return totalBuckets;
	}

	/**
	 * 将给定的基数估计器（包括当前对象）分为普通存储格式和稀疏存储格式两类
	 * 
	 * @param estimators
	 * @param normEsts
	 * @param sparseEsts
	 */
	final void classifyBmp(ICardinality[] estimators,
			List<AdaptiveCounting> normEsts, List<AdaptiveCounting> sparseEsts) {
		// 将当前估计器对象也加入分类列表
		if (isSparse()) {
			sparseEsts.add(this);
		} else {
			normEsts.add(this);
		}

		// 对待合并的基数估计器进行分类
		for (ICardinality est : estimators) {
			if ((est instanceof AdaptiveCounting) && est.sizeof() == m) {
				AdaptiveCounting ac = (AdaptiveCounting) est;
				if (ac.isSparse()) {
					sparseEsts.add(ac);
				} else {
					normEsts.add(ac);
				}
			} else {
				throw new IllegalArgumentException(
						"Unable merge AdaptiveCounting with "
								+ est.getClass().getName() + " and length "
								+ est.sizeof());
			}
		}
	}

	/**
	 * 合并给定的普通位图和稀疏位图，最终生成普通位图
	 * 
	 * @param normEsts
	 * @param sparseEsts
	 * @return
	 */
	final byte[] mergeBmpGenNorm(List<AdaptiveCounting> normEsts,
			List<AdaptiveCounting> sparseEsts) {
		byte[] mergedBytes = new byte[m];

		// 合并所有普通存储格式位图
		if (normEsts != null) {
			for (int i = 0; i < normEsts.size(); i++) {
				byte[] toMerge = normEsts.get(i).M;
				for (int j = 0; j < toMerge.length; j++) {
					if (mergedBytes[j] < toMerge[j]) {
						mergedBytes[j] = toMerge[j];
					}
				}
			}
		}

		// 合并所有稀疏存储格式位图，注意由于位图最大长度 m 保证相同，故桶下标字节数也肯定相同，直接使用当前估计器的设置即可
		if (sparseEsts != null) {
			for (int i = 0; i < sparseEsts.size(); i++) {
				AdaptiveCounting ac = sparseEsts.get(i);
				byte[] toMerge = ac.M;
				int curLen = ac.usedLen();

				for (int j = 1; j < curLen; j += sparseIdxLen + 1) {
					byte r = toMerge[j];
					int idx = bytesToInt(toMerge, j + 1, sparseIdxLen);
					if (mergedBytes[idx] < r) {
						mergedBytes[idx] = r;
					}
				}
			}
		}

		return mergedBytes;
	}

	/**
	 * 多路归并方式合并给定的稀疏格式基数估计器
	 * 
	 * @param finalBkts
	 * @param sparseEsts
	 * @return
	 */
	final byte[] mergeBmpGenSparse(int finalBkts,
			List<AdaptiveCounting> sparseEsts) {
		int step = sparseIdxLen + 1;
		byte[] mergedBytes = new byte[finalBkts * step + 1];

		// 生成 ID
		mergedBytes[0] = genSparseId(k);

		// 包括当前对象在内的位图遍历游标及最大长度数据
		int estNo = sparseEsts.size();
		int[] offs = new int[estNo];
		byte[][] bmps = new byte[estNo][];
		int[] curLens = new int[estNo];

		for (int i = 0; i < estNo; i++) {
			AdaptiveCounting ac = sparseEsts.get(i);
			byte[] sm = ac.M;
			bmps[i] = sm;
			offs[i] = 1;
			curLens[i] = ac.usedLen();
		}

		int idx;
		int minIdx;
		int minEstNo = 0;
		byte minR = 0;
		int lastIdx = -1;
		int off = 1;

		// 多路归并方式合并稀疏位图
		while (minEstNo != -1) {
			minIdx = Integer.MAX_VALUE;
			minEstNo = -1;
			for (int i = 0; i < estNo; i++) {
				if (offs[i] < curLens[i]) {
					idx = bytesToInt(bmps[i], offs[i] + 1, sparseIdxLen);
					if (idx < minIdx) {
						minIdx = idx;
						minEstNo = i;
						minR = bmps[i][offs[i]];
					}
				}
			}

			if (minEstNo != -1) {
				if (lastIdx != minIdx) {
					if (lastIdx != -1) {
						off += step;
					}
					intToBytes(mergedBytes, off + 1, sparseIdxLen, minIdx);
					lastIdx = minIdx;
				}
				if (mergedBytes[off] < minR) {
					mergedBytes[off] = minR;
				}
				offs[minEstNo] += step;
			}
		}

		return mergedBytes;
	}

	/**
	 * 判断当前桶位图是否为稀疏存储格式
	 * 
	 * @return
	 */
	public final boolean isSparse() {
		return (M[0] & 0x80) != 0;
	}

	/**
	 * 返回稀疏/普通位图当前使用的字节数
	 * 
	 * @return
	 */
	public final int usedLen() {
		if (isSparse()) {
			return (m - b_e) * (sparseIdxLen + 1) + 1;
		} else {
			return m;
		}
	}

	/**
	 * 返回稀疏/普通位图真正使用的字节数
	 * 
	 * @return
	 */
	public final int realLen() {
		return M.length;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format(
				"k=%d, m=%d, sparseIdxLen=%d, b_e=%d, Rsum=%d\n", k, m,
				sparseIdxLen, b_e, Rsum));
		if (isSparse()) {
			sb.append(String.format("Sparse bitmap: len=%d, ID=%#02x\n",
					usedLen(), M[0]));
			for (int i = 1; i < usedLen(); i += sparseIdxLen + 1) {
				int r = M[i];
				int idx = bytesToInt(M, i + 1, sparseIdxLen);
				sb.append(String.format("[%d]=%d | ", idx, r));
			}
		} else {
			sb.append(String.format("Normal bitmap: len=%d", M.length));
		}
		return sb.toString();
	}

	/**
	 * 判断是否应转换为普通存储格式
	 * 
	 * @return
	 */
	final boolean shouldUseNormalBmp(int usedBkts) {
		if ((usedBkts + 1) * (sparseIdxLen + 1) >= m) {
			return true;
		}
		return false;
	}

	/**
	 * 将稀疏存储格式转换为普通存储格式
	 */
	final void convertSparseToNormalBmp() {
		if (!isSparse()) {
			throw new IllegalArgumentException(
					"Can't convert non-sparse bitmap to normal bitmap!");
		}

		byte[] newM = new byte[m];
		int curLen = (m - b_e) * (sparseIdxLen + 1) + 1;
		for (int i = 1; i < curLen; i += sparseIdxLen + 1) {
			int idx = bytesToInt(M, i + 1, sparseIdxLen);
			newM[idx] = M[i];
		}

		M = newM;
	}

	/**
	 * 向稀疏格式位图插入指定编号的桶数据
	 * 
	 * @param bktIdx
	 * @param r
	 */
	final void sparseInsertBucket(int bktIdx, byte r) {
		int step = sparseIdxLen + 1;
		int nBuckets = m - b_e;
		int begin = 0, end = nBuckets, mid;

		// 二分查找待插入桶的位置
		while (begin <= end && begin < nBuckets && end >= 0) {
			mid = (begin + end) >> 1;
			int off = mid * step + 1;
			int idx = bytesToInt(M, off + 1, sparseIdxLen);
			if (bktIdx == idx) {
				throw new IllegalArgumentException("impossible case");
			} else if (bktIdx < idx) {
				end = mid - 1;
			} else {
				begin = mid + 1;
			}
		}

		// 必要时扩展稀疏位图长度
		if ((nBuckets + 1) * step >= M.length) {
			// 每次扩充位图到原来的 2 倍长，最大不超过 m
			int newLen = Math.min(M.length << 1, m);
			byte[] tmpM = new byte[newLen];
			System.arraycopy(M, 0, tmpM, 0, M.length);
			M = tmpM;
		}
		// 必要时插入点后的数据后移
		int off = begin * step + 1;
		if (begin < nBuckets) {
			System.arraycopy(M, off, M, off + step, nBuckets * step + 1 - off);

		}
		// 插入新桶
		M[off] = r;
		intToBytes(M, off + 1, sparseIdxLen, bktIdx);
	}

	/**
	 * 使用二分法在稀疏存储结构中查找给定桶下标的偏移量
	 * 
	 * @param bktIdx
	 * @return -1 表示未找到给定下标的桶
	 */
	final int sparseBisectSearch(int bktIdx) {
		int step = sparseIdxLen + 1;
		int nBuckets = m - b_e;
		int begin = 0, end = nBuckets, mid;

		// 二分查找待更新的桶
		while (begin <= end && begin < nBuckets && end >= 0) {
			mid = (begin + end) >> 1;
			int off = mid * step + 1;
			int idx = bytesToInt(M, off + 1, sparseIdxLen);
			if (bktIdx == idx) {
				return off;
			} else if (bktIdx < idx) {
				end = mid - 1;
			} else {
				begin = mid + 1;
			}
		}

		return -1;
	}

	/**
	 * 将给定位置处的指定长度字节序列按 little-endian 字节序转换为整数
	 * 
	 * @param bytes
	 * @param off
	 * @param len
	 * @return
	 */
	final static int bytesToInt(byte[] bytes, int off, int len) {
		if (len == 2) {
			return (bytes[off] & 0xff) | (bytes[off + 1] & 0xff) << 8;
		} else if (len == 1) {
			return bytes[off] & 0xff;
		}

		int r = 0;
		for (int i = len - 1; i >= 0; i--) {
			r = (r << 8) | (bytes[off + i] & 0xff);
		}
		return r;
	}

	/**
	 * 将给定整数以 little-endian 字节序列的形式写入给定位图的指定位置
	 * 
	 * @param bytes
	 * @param off
	 * @param r
	 */
	final static void intToBytes(byte[] bytes, int off, int len, int r) {
		if (len == 2) {
			bytes[off] = (byte) (r & 0xff);
			bytes[off + 1] = (byte) ((r >> 8) & 0xff);
			return;
		} else if (len == 1) {
			bytes[off] = (byte) (r & 0xff);
			return;
		}

		while (len > 0) {
			bytes[off] = (byte) (r & 0xff);
			r = r >> 8;
			off++;
			len--;
		}
	}

	/**
	 * 计算给定桶数量后保存桶下标所需的最小字节数
	 * 
	 * @param k
	 *            以log2(m)表示的桶数量
	 * @return
	 */
	final static int calcBktIdxBytes(int k) {
		return (int) Math.ceil(k / 8.0);
	}

	/**
	 * 生成稀疏存储格式 ID 字节
	 * 
	 * @param k
	 *            以log2(m)表示的桶数量
	 * @return
	 */
	final static byte genSparseId(int k) {
		return (byte) (k | 0x80);
	}

	/**
	 * 从稀疏存储格式 ID 字节提取 log2(m)
	 * 
	 * @param id
	 * @return
	 */
	final static int kFromSparseId(byte id) {
		return id & 0x7f;
	}
}

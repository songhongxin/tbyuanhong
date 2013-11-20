package com.etao.lz.star;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.iterator.TShortObjectIterator;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.ClassUtils;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.etao.lz.storm.utils.GZipUtil;

public class ConvUtil {

	public static Object recurTroveToStd(Object m) {
		return recurTroveToStd(m, false);
	}

	/**
	 * 将嵌套的 Trove 容器类转换为嵌套的标准 Map，并将内部的指标类转换为 Map<String,Object>
	 * 
	 * @param m
	 *            嵌套的 Trove 容器对象
	 * @param dumpBmp
	 *            是否输出 UV 字段对应的压缩位图
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static Object recurTroveToStd(Object m, boolean dumpBmp) {
		if (m == null) {
			return null;
		}

		if (m instanceof TShortObjectHashMap) {
			TShortObjectHashMap troveM = (TShortObjectHashMap) m;
			Map<Short, Object> resM = new TreeMap<Short, Object>();

			for (TShortObjectIterator it = troveM.iterator(); it.hasNext();) {
				it.advance();
				Object val = recurTroveToStd(it.value(), dumpBmp);
				resM.put(it.key(), val);
			}

			return resM;
		} else if (m instanceof TIntObjectHashMap) {
			TIntObjectHashMap troveM = (TIntObjectHashMap) m;
			Map<Integer, Object> resM = new TreeMap<Integer, Object>();

			for (TIntObjectIterator it = troveM.iterator(); it.hasNext();) {
				it.advance();
				Object val = recurTroveToStd(it.value(), dumpBmp);
				resM.put(it.key(), val);
			}

			return resM;
		} else if (m instanceof TLongObjectHashMap) {
			TLongObjectHashMap troveM = (TLongObjectHashMap) m;
			Map<Long, Object> resM = new TreeMap<Long, Object>();

			for (TLongObjectIterator it = troveM.iterator(); it.hasNext();) {
				it.advance();
				Object val = recurTroveToStd(it.value(), dumpBmp);
				resM.put(it.key(), val);
			}

			return resM;
		} else {
			// 应该是 GrdInds 等指标容器类，只需要特别考虑 UV 相关指标对应的 AdaptiveCounting 类型即可
			Map<String, Object> resM = new TreeMap<String, Object>();

			// 使用反射消除遍历指标类成员时对类型的依赖
			Field[] objFields = m.getClass().getFields();
			for (Field objField : objFields) {
				String key = objField.getName();
				Object value = null;
				try {
					value = objField.get(m);
				} catch (Exception e) {
					// 忽略 AccessException
				}

				if (value instanceof AdaptiveCounting) {
					if (!key.endsWith("b")) {
						// XXX UV 指标命名必须以 "b" 结尾！
						throw new RuntimeException(
								String.format(
										"UV indicator %s doesn't confirm naming rule, must ending with 'b'",
										key));
					}
					// 为 UV 指标输出基数估计值，对应键名为将结尾的 "b" 去除后的名字
					AdaptiveCounting ac = (AdaptiveCounting) value;
					String nkey = key.substring(0, key.length() - 1);
					resM.put(nkey, ac.cardinality());
					if (dumpBmp) {
						// 需要输出 UV 位图时对应的键名就是字段名，位图要进行 GZip 压缩
						resM.put(key, GZipUtil.GZip(ac.getBytes()));
					}
				} else {
					resM.put(key, value);
				}
			}

			return resM;
		}
	}

	/**
	 * 将嵌套的 Map 容器转换为特定指标类型的嵌套 Trove 容器
	 * 
	 * FIXME: 当前为了简单可靠地判定 Trove 容器类型，默认对所有中间层级使用传入的 Trove 类，若不符合此要求则需要修正！
	 * 
	 * @param m
	 *            嵌套的 Map 容器
	 * @param troveClazz
	 *            结果 Trove 容器类
	 * @param indClazz
	 *            内部指标类型类
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Object stdToTroveNoGid(Object m, Class troveClazz,
			Class indClazz) throws Exception {
		if (m == null) {
			return null;
		}

		Map<?, ?> map = (Map<?, ?>) m;
		// 获取给定 Map 的首个 key 以便确定目标对象类型
		Object firstKey = null;
		for (Entry<?, ?> entry : map.entrySet()) {
			firstKey = entry.getKey();
			break;
		}

		Object resT = null;
		if (firstKey instanceof String) {
			// 首 key 为 String，目标对象是指标对象
			resT = indClazz.newInstance();
			for (Field field : indClazz.getFields()) {
				String fieldName = field.getName();
				Object value = map.get(fieldName);

				if (value != null) {
					// XXX: 获得字段类型类时需要将可能的原生类型类转换为对应的包装器类
					Class fieldTypeClazz = ClassUtils.primitiveToWrapper(field
							.getType());
					if (fieldTypeClazz == AdaptiveCounting.class) {
						// 基数估计位图字段
						AdaptiveCounting ac = new AdaptiveCounting(
								GZipUtil.unGZip((byte[]) value));
						field.set(resT, ac);
					} else {
						// 数值字段类型转换太麻烦，统一用字符串传入对应类型的 valueOf() 方法得到合适类型的对象
						Method valOf = null;
						try {
							valOf = fieldTypeClazz.getMethod("valueOf",
									String.class);
						} catch (Exception e) {
							// String.valueOf(...) 只接收 Object 类型的参数
							valOf = fieldTypeClazz.getMethod("valueOf",
									Object.class);
						}
						// 调用对应类型的 valueOf() 方法将字段字符串转换为对应类型
						Object convVal = valOf.invoke(null, value.toString());
						field.set(resT, convVal);
					}
				}
			}
		} else {
			// 首 key 非 String，目标对象是 Trove 容器
			resT = troveClazz.newInstance();

			// 遍历当前层级 k/v 对并插入 Trove 容器
			for (Entry<?, ?> entry : map.entrySet()) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				Object child = stdToTroveNoGid(value, troveClazz, indClazz);

				if (troveClazz == TByteObjectHashMap.class) {
					((TByteObjectHashMap) resT).put(
							Byte.valueOf(key.toString()), child);
				} else if (troveClazz == TShortObjectHashMap.class) {
					((TShortObjectHashMap) resT).put(
							Short.valueOf(key.toString()), child);
				} else if (troveClazz == TIntObjectHashMap.class) {
					((TIntObjectHashMap) resT).put(
							Integer.valueOf(key.toString()), child);
				} else if (troveClazz == TLongObjectHashMap.class) {
					((TLongObjectHashMap) resT).put(
							Long.valueOf(key.toString()), child);
				} else {
					throw new IllegalArgumentException(String.format(
							"Invalid map key type: %s", key.getClass()
									.toString()));
				}
			}
		}

		return resT;
	}

}

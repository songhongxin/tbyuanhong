storm kill star-flow
storm kill star-business
svn up
ant
echo "Waiting to restart...."
sleep 20 
storm jar star-storm.jar com.etao.lz.star.StarMain star-flow flow
storm jar star-storm.jar com.etao.lz.star.StarMain star-business business


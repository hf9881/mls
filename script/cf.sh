echo $@

TEMPDIR="/test/tmp/"  #存储中间数据的主目录

 #运行jar包
echo "hadoop jar /home/paas/script/jar/cf.jar $@ --tempDir $TEMPDIR --optLevel 0"
hadoop jar /home/paas/script/jar/cf.jar $@ --tempDir $TEMPDIR --optLevel 0 

echo "exit!"
echo $@

TEMPDIR="/test/tmp/"  #存储中间数据的主目录

 #运行jar包
echo "hadoop jar /home/paas/script/jar/ar.jar $@ --tempDir $TEMPDIR" 
hadoop jar /home/paas/script/jar/ar.jar $@ --tempDir $TEMPDIR

echo "exit!"

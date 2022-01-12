#!/bin/bash
# 在集群的所有机器上批量执行同一条命令
if(($#==0))
then
  echo 请输入您要操作的命令
  exit
fi

echo 要执行的命令是$*

# 循环执行此命令
for((i=1;i<=10;i++))
do
  echo ======= yw-datanode0$i =======
  ssh yw-datanode0$i $*
done
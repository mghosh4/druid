for i in `seq 1 12`;
do
  ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no xiaoyao@node-$i.druidxiaoyao.dcsq.emulab.net "sudo chsh -s /bin/bash xiaoyao"
done

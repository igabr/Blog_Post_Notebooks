#!/bin/bash
set -x -e #https://explainshell.com/explain?cmd=set+-x+-e

#updates **Linux** Distribution
sudo yum -y update

#Default password set to password123 unless a cmd line argument in position 1 is specified.
JUPYTER_PASSWORD=${1:-"password123"}
#default port for jupyter notebook set to 8194 unless specified by cmd line argument in position 2
PORT=${2:-8194}

#Install miniconda - miniconda > anaconda as its way smaller. No memory issues will occur. As such avoids needless shifting of directories.
sudo wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh #https://explainshell.com/explain?cmd=sudo+wget+https%3A%2F%2Frepo.continuum.io%2Fminiconda%2FMiniconda3-latest-Linux-x86_64.sh+-O+~%2Fminiconda.sh
# Execute the created miniconda.sh script
bash ~/miniconda.sh -b -p $HOME/miniconda
echo -e '\nexport PATH=$HOME/miniconda/bin:$PATH' >> $HOME/.bashrc #need to update path so we can use conda in the command line.
source $HOME/.bashrc #Dont forget to source here - otherwise everything will break!
echo -e '\nexport SPARK_HOME=/usr/lib/spark' >> $HOME/.bashrc # spark lives here
echo -e "\nexport PATH=$PATH:$SPARK_HOME/bin" >> $HOME/.bashrc # need to append to path so that Jupyter and our command line know where to find and execute spark
echo -e '\nexport PYSPARK_PYTHON="/home/hadoop/miniconda/bin/python3.6"' >> $HOME/.bashrc # tells pyspark to run on python 3.6
source $HOME/.bashrc
#My preference of aliases
echo -e '\nalias c="clear"' >> $HOME/.bashrc 
echo -e "\nalias py='ipython3'" >> $HOME/.bashrc
echo -e "\nalias ls='ls -hlpGF'" >> $HOME/.bashrc
source $HOME/.bashrc
conda update -y conda # update miniconda
rm ~/miniconda.sh # This removes the script we used to install miniconda.

conda install -y boto3 numpy pandas scipy scikit-learn statsmodels ipython # packages to be installed on every EC2 instance. Both the Master and the Slave Node.
# Recall that this script is run on BOTH the Master Node and the Slave nodes at THE SAME TIME.

## The code below will ONLY be run on the master node.
IS_MASTER=false
if grep isMaster /mnt/var/lib/info/instance.json | grep true; #This segment of code identifies if an EC2 instance is labeled as the Master.
then
  IS_MASTER=true

  sudo yum install -y git #install git on master node
  echo "Installing Python libs for Master" # this echo is useful for investigating logs if bootstrap fails.
  conda install -y jupyter matplotlib seaborn #install jupyter and viz packages on master. No need to be done on slaves.
  jupyter notebook --generate-config #Jupyter config stuff below
  HASHED_PASSWORD=$(python -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))")
  sed -i '/c.NotebookApp.port/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.port = $PORT" >> ~/.jupyter/jupyter_notebook_config.py
  sed -i '/c.NotebookApp.ip/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.ip = '*'" >> ~/.jupyter/jupyter_notebook_config.py
  sed -i '/c.NotebookApp.open_browser/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
  sed -i '/c.NotebookApp.MultiKernelManager.default_kernel_name/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.MultiKernelManager.default_kernel_name = 'pyspark'" >> ~/.jupyter/jupyter_notebook_config.py #default kernel set to PySpark
  sed -i '/c.NotebookApp.password/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.password = u'$HASHED_PASSWORD'" >> ~/.jupyter/jupyter_notebook_config.py
  echo -e '\nexport PYSPARK_DRIVER_PYTHON=jupyter' >> $HOME/.bashrc
  echo -e '\nexport PYSPARK_DRIVER_PYTHON_OPTS="notebook"' >> $HOME/.bashrc
  echo -e '\nalias jn="jupyter notebook"' >> $HOME/.bashrc
  echo -e '\nalias ps="pyspark"' >> $HOME/.bashrc
  source $HOME/.bashrc
fi


############## AWS CLI EXECUTION - below is how you would execute a cluster launch using cli

# aws emr create-cluster --name "Spark Cluster" \
# --release-label emr-5.11.1 \
# --use-default-roles \
# --log-uri <insert s3 path here for your EMR logs to be stored> \
# --applications Name=Spark Name=Ganglia \
# --ec2-attributes \
#     KeyName=<insert name of .pem key here>,SubnetId=<insert subnet ID here>,EmrManagedMasterSecurityGroup=<insert master node SG here>,EmrManagedSlaveSecurityGroup=<insert Slave Nodes SG here> \
# --instance-type=m4.large \
# --instance-count 3 \
# --region us-east-1 \
# --bootstrap-action Path=<insert s3 path to bootstrap here>,Args=[<insert bootstrap script cmd line args here>] \



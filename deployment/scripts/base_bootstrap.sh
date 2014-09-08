#set up vars and init logfiles
LOGFILE=/tmp/bootstrap.log
FFDIR=/opt/balihoo/fulfillment
echo running bootstrap script > ${LOGFILE} 2>&1
echo ${PATH}>> ${LOGFILE} 2>&1

echo export aws keys >> ${LOGFILE} 2>&1
export AWS_ACCESS_KEY_ID=${AWSACCESSKEY}
export AWS_SECRET_ACCESS_KEY=${AWSSECRETKEY}

echo downloading cli tools >> ${LOGFILE} 2>&1
curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o awscli-bundle.zip >> ${LOGFILE} 2>&1
unzip awscli-bundle.zip >> ${LOGFILE} 2>&1
echo unzipped bundle >> ${LOGFILE} 2>&1
./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws >> ${LOGFILE} 2>&1
echo installed bundle >> ${LOGFILE} 2>&1

echo downloading fulfillment application >> ${LOGFILE} 2>&1
mkdir -p ${FFDIR} >> ${LOGFILE} 2>&1
/usr/local/bin/aws s3 sync ${S3BUCKETURL} ${FFDIR} >> ${LOGFILE} 2>&1

echo setting install script execute permissions >> ${LOGFILE} 2>&1
chmod a+x ${FFDIR}/ffinstall >> ${LOGFILE} 2>&1

echo installing fulfillment application >> ${LOGFILE} 2>&1
nohup ${FFDIR}/ffinstall ${CLASSNAMES} >> ${LOGFILE} 2>&1 &
echo done >> ${LOGFILE} 2>&1

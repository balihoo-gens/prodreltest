#!/bin/bash\n
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
wget https://s3.amazonaws.com/aws-cli/awscli-bundle.zip >> \\${LOGFILE} 2>&1\n",
"echo downloaded bundle >> \\${LOGFILE}\n",
"unzip awscli-bundle.zip >> \\${LOGFILE} 2>&1\n",
"echo unzipped bundle >> \\${LOGFILE}\n",
"./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws >> \\${LOGFILE} 2>&1\n",
"echo installed bundle >> \\${LOGFILE}\n",
"if [ -x /usr/local/bin/aws ]\n",
"then\n",
"  echo aws exists: >> \\${LOGFILE}\n",
"  /usr/local/bin/aws --version >> \\${LOGFILE} 2>&1\n",
"  cd /tmp\n",
"  echo moved to PWD \\$(pwd) >> \\${LOGFILE}\n",
"  echo downloading app \\${APPFILE}... >> \\${LOGFILE}\n",
"  /usr/local/bin/aws s3 cp s3://balihoo.dev.fulfillment/\\${APPFILE} . >> \\${LOGFILE} 2>&1\n",
"  if [ -f \\${APPFILE} ]\n",
"  then\n",
"    echo downloaded ff app >> \\${LOGFILE}\n",
"    tar xvzf \\${APPFILE} >> \\${LOGFILE} 2>&1\n",
"    echo extracted app tarball >> \\${LOGFILE}\n",
"    if [ -d installtmp ]\n",
"    then\n",
"      mkdir -p /opt/balihoo\n",
"      echo created opt dir >> \\${LOGFILE}\n",
"      mv installtmp /opt/balihoo/fulfillment\n",
"      if [ -d /opt/balihoo/fulfillment ]\n",
"      then\n",
"        echo moved app >> \\${LOGFILE}\n",
"        cd /opt/balihoo/fulfillment\n",
"        mkdir logs\n",
"        if [ -d logs ]\n",
"        then\n",
"          echo created logs dir >> \\${LOGFILE}\n",
"          nohup ./launch_fulfillment fulfillment.jar > logs/launch.log 2>&1 &\n",
"          PID=\\$!\n",
"          #this kill does not kill, just tests the pid\n",
"          if \\$(kill -s 0 \\$PID)\n",
"          then\n",
"            echo started app with PID \\$PID >> \\${LOGFILE}\n",
"          else\n",
"            echo ERROR failed to start app >> \\${LOGFILE}\n",
"          fi\n",
"        else\n",
"          echo ERROR failed to mkdir /opt/balihoo/fulfillment/logs >> \\${LOGFILE}\n",
"        fi\n",
"      else\n",
"        echo ERROR /opt/balihoo/fulfillment does not exist >> \\${LOGFILE}\n",
"      fi\n",
"    else\n",
"      echo ERROR failed to extract app tarball >> \\${LOGFILE}\n",
"    fi\n",
"  else\n",
"    echo ERROR failed to download app tarball >> \\${LOGFILE}\n",
"  fi\n",
"else\n",
"  echo ERROR aws executable not present >> \\${LOGFILE}\n",
"fi\n",
"echo done >> \\${LOGFILE}\n",


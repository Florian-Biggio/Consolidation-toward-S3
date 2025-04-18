FROM mongo:4.4
WORKDIR /usr/src/configs
COPY replicaSet.js .
COPY setup.sh .

ENTRYPOINT [ "/bin/bash" ]
CMD ["./setup.sh"]

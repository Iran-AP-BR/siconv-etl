FROM mysql:5.7.38

RUN echo "PS1='\u @ \e[32msiconv-data-db\e[m \e[36m\w \$\e[m '" >> /etc/bash.bashrc

WORKDIR /home/db

CMD ["mysqld", "--explicit_defaults_for_timestamp", "--tls-version=TLSv1.2"]

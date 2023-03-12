FROM python:3.8

RUN apt-get update -y && \
    apt-get install tini -y

RUN  echo "PS1='\u @ \e[33msiconv-data-api\e[m \e[36m\w \$\e[m '" >> /etc/bash.bashrc

WORKDIR /home

COPY ./etl/requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

COPY ./requirements-TEST.txt .
RUN pip install -r requirements-TEST.txt

COPY . .

ENTRYPOINT ["tini", "--"]

CMD [ "python", "test.py" ]

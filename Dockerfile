FROM akariv/dgp-server:latest

ADD taxonomies /dgp/taxonomies/
ADD dataflows_openspending /dgp/dataflows_openspending/

ADD server-requirements.txt /dgp
RUN python -m pip install -r /dgp/server-requirements.txt

ENV SERVER_MODULE=dataflows_openspending.server:app

WORKDIR /dgp/

FROM nginx:1.21.3-alpine

RUN rm /etc/nginx/conf.d/default.conf

ENV HOME=/home
ENV APP_HOME=/home/app
RUN mkdir $APP_HOME
RUN mkdir $APP_HOME/static
WORKDIR $APP_HOME
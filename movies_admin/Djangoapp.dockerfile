FROM python:3.9-alpine

ADD ./movies_admin/requirements.txt /app/requirements.txt

RUN set -ex \
    && apk add --no-cache --virtual .build-deps postgresql-dev build-base \
    && python -m venv /env \
    && /env/bin/pip install --upgrade pip \
    && /env/bin/pip install --no-cache-dir -r /app/requirements.txt \
    && runDeps="$(scanelf --needed --nobanner --recursive /env \
        | awk '{ gsub(/,/, "\nso:", $2); print "so:" $2 }' \
        | sort -u \
        | xargs -r apk info --installed \
        | sort -u)" \
    && apk add --virtual rendeps $runDeps \
    && apk del .build-deps

ADD ./movies_admin /app
WORKDIR /app

ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

CMD ["gunicorn", "--bind", ":${DJANGO_APP_PORT}", "--workers", "${GUNICORN_WORKERS}", "config.wsgi:application"]
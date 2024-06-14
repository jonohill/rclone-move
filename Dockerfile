FROM rclone/rclone:1.67.0

RUN apk add --no-cache --update \
        py3-requests \
        python3

ENV PYTHONUNBUFFERED=1

ADD https://raw.githubusercontent.com/jonohill/plex-refresh/main/plex_refresh.py /plex_refresh.py
COPY entrypoint.py /entrypoint.py
ENTRYPOINT [ "/entrypoint.py" ]

FROM rclone/rclone:1.61.1

RUN apk add --no-cache --update \
        py3-requests \
        python3

RUN touch /__init__.py
ADD https://raw.githubusercontent.com/jonohill/plex-refresh/main/plex_refresh.py /plex_refresh.py
COPY entrypoint.py /entrypoint.py
ENTRYPOINT [ "/entrypoint.py" ]

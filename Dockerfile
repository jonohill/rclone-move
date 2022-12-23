FROM rclone/rclone:1.61.1

RUN apk add --no-cache --update \
        python3

COPY entrypoint.py /entrypoint.py
ENTRYPOINT [ "/entrypoint.py" ]

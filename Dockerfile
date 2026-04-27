FROM ghcr.io/jiho7407/hadoop-base:3.3.6

COPY docker/entrypoint.sh /usr/local/bin/hadoop-entrypoint

RUN chmod +x /usr/local/bin/hadoop-entrypoint

USER hadoop
WORKDIR /opt/hadoop

ENTRYPOINT ["hadoop-entrypoint"]
CMD ["bash"]

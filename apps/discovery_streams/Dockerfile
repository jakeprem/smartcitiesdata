FROM smartcitiesdata:build as builder
RUN MIX_ENV=prod mix distillery.release --name discovery_streams

FROM bitwalker/alpine-elixir:1.8.1
ENV REPLACE_OS_VARS=true
RUN apk upgrade && \
    apk add --no-cache bash openssl && \
    rm -rf /var/cache/**/*
WORKDIR /app
COPY --from=builder /app/_build/prod/rel/discovery_streams/ .
ENV PORT 4000
EXPOSE ${PORT}
CMD ["bin/discovery_streams", "foreground"]

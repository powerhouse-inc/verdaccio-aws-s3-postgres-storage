###############################################
# Stage 1 – build the plugin
###############################################
FROM node:24-alpine AS builder

RUN corepack enable

WORKDIR /plugin
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN corepack install
RUN pnpm install --frozen-lockfile

COPY tsconfig.json vite.config.ts ./
COPY src/ ./src/
RUN pnpm build

# Prune dev dependencies for a lean install
RUN pnpm prune --prod

###############################################
# Stage 2 – verdaccio + plugin
###############################################
FROM verdaccio/verdaccio:7.x-next

USER root

# Copy the built plugin into verdaccio's plugin directory
ARG PLUGIN_DIR=/verdaccio/plugins/@powerhousedao/verdaccio-s3-storage
RUN mkdir -p $PLUGIN_DIR
COPY --from=builder /plugin/lib/          $PLUGIN_DIR/lib/
COPY --from=builder /plugin/package.json  $PLUGIN_DIR/

COPY --from=builder /plugin/node_modules/ $PLUGIN_DIR/node_modules/

# Bake the default config into the image
COPY conf/config.yaml /verdaccio/conf/config.yaml

RUN chown -R 10001:65533 /verdaccio/plugins /verdaccio/conf

USER 10001

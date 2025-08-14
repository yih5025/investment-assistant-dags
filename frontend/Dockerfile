# Multi-stage build for frontend: build at image build time, serve with nginx

# 1) Builder stage: install deps and build static files
FROM node:20-alpine AS builder
WORKDIR /app

# Install OS deps if needed
RUN apk add --no-cache python3 make g++

# Copy package manifests first for better caching
COPY package*.json ./
RUN npm ci --no-audit --no-fund

# Copy source and build
COPY . .
ENV VITE_API_BASE="https://api.investment-assistant.site/api/v1"
ENV VITE_WS_BASE="wss://api.investment-assistant.site/api/v1/ws"
RUN npm run build

# 2) Runtime stage: lightweight nginx to serve built assets
FROM nginx:1.27-alpine AS runtime

# Copy nginx config provided by k8s ConfigMap at runtime if desired; default fallback
COPY --from=builder /app/dist /usr/share/nginx/html

# Simple default health endpoint in case external config not mounted
RUN printf 'server {\n  listen 80;\n  root /usr/share/nginx/html;\n  location / { try_files $uri $uri/ /index.html; }\n  location /health { return 200 "healthy\\n"; add_header Content-Type text/plain; }\n}\n' > /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]



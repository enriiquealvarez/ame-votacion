FROM node:22-alpine
WORKDIR /app
COPY . .
ENV PORT=3000 DATA_DIR=/app/data TZ=America/Mexico_City
EXPOSE 3000
CMD ["node", "server.js"]

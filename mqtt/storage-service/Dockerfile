FROM node:20-alpine

WORKDIR /app/mqtt/storage-service

COPY mqtt/storage-service/package*.json ./
RUN npm install --omit=dev

COPY mqtt/storage-service/src ./src

CMD ["node", "src/index.js"]

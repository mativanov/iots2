FROM node:20-alpine

WORKDIR /app/mqtt/ingestion-service

COPY mqtt/ingestion-service/package*.json ./
RUN npm install --omit=dev

COPY shared/event-generator /app/shared/event-generator
COPY shared/sample-data /app/shared/sample-data
COPY mqtt/ingestion-service/src ./src

CMD ["node", "src/index.js"]

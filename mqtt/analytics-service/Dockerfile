FROM node:20-alpine

WORKDIR /app/mqtt/analytics-service

COPY mqtt/analytics-service/package*.json ./
RUN npm install --omit=dev

COPY mqtt/analytics-service/src ./src

CMD ["node", "src/index.js"]

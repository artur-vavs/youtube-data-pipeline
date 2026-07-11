FROM node:20-alpine

WORKDIR /app

# Instala as dependencias primeiro (camada cacheavel).
COPY package.json ./
RUN npm install

# Copia o restante do projeto (codigo + data/gold.json).
COPY . .

ENV NEXT_TELEMETRY_DISABLED=1
EXPOSE 3000

# Sobe o servidor de desenvolvimento acessivel fora do container.
CMD ["npm", "run", "dev", "--", "-H", "0.0.0.0", "-p", "3000"]

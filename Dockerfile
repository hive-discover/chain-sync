FROM node:16.4.2-buster-slim

RUN apt-get update && apt-get install -y python3 g++ make

WORKDIR /NodeJS
COPY package.json .
RUN npm install --production
RUN npm prune --production

# Solve the problem by reinstaling bcrypt
RUN npm uninstall bcrypt
RUN npm i bcrypt

COPY . .
ENTRYPOINT [ "node", "listener.js" ] 
# registry.hive-discover.tech/chain-sync:0.1.7.5.4
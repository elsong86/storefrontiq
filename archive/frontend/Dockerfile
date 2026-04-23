# frontend/Dockerfile
FROM node:22.6.0 AS frontend

WORKDIR /frontend

# Copy only package.json and package-lock.json to install dependencies first
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application
COPY . .

# Expose the port
EXPOSE 3000

# Command to run
CMD ["npm", "run", "dev"]

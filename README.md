### operando os containers:
  
```bash
# removendo tudo:
docker-compose down --rmi all --volumes --remove-orphans
docker system prune -a --volumes -f

# validando remoção:
docker ps -a  # Verifica se ainda há containers parados
docker images  # Verifica se ainda há imagens
docker volume ls  # Verifica se ainda há volumes

# removendo se necessário:
docker rm -f $(docker ps -aq)  # Remove todos os containers
docker rmi -f $(docker images -q)  # Remove todas as imagens
docker volume rm $(docker volume ls -q)  # Remove todos os volumes

# reconstruindo forçando pull das imagens:
docker-compose build --no-cache
docker-compose up -d
```

### comandos para ler o tópico

```bash
# confirmando criação do tópico:
docker exec -it kafka-server /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka-server:9092

# detalhes do tópico:
docker exec -it kafka-server /opt/kafka/bin/kafka-topics.sh --describe --topic app-events --bootstrap-server kafka-server:9092

# consumindo mensagens:
docker exec -it kafka-server /opt/kafka/bin/kafka-console-consumer.sh --topic app-events --from-beginning --bootstrap-server kafka-server:9092
```

### acessando banco sql:
```bash
docker exec -it events_storage mysql -u root -p
# senha: root
```


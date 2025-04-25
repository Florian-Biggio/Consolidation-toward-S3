# Les scripts

Réalise la migration des données présentes sur un bucket S3 d'AWS vers une DB mongoDB 

Necessite un fichier secret.json tel que :

```
{
    "AWS_ACCESS_KEY_ID": "ABCDEFGHAPI",
    "AWS_SECRET_ACCESS_KEY": "abcdefghapiapiapiapiapiapi",
    "AWS_REGION": "eu-north-1"
}
```
placé dans le dossier /migration/


Pour forcer un schema sur notre future DB :
```
py migration/create_collection.py --mongodb_address mongodb://localhost:27017/ -v INFO
```
/!\ Drop la collection si elle existe

Les scripts se lancent ainsi:
```
py migration/xlsx.py Ichtegem --mongodb_address mongodb://localhost:27017/ -v INFO
py migration/xlsx.py Madeleine --mongodb_address mongodb://localhost:27017/ -v INFO
py migration/jsonl.py InfoClimat --mongodb_address mongodb://localhost:27017/ -v INFO
```
pour lancer la migration des 3 fichiers, avec les arguments optionnels :
--mongodb_address : mongodb://localhost:27017/ par défaut. Addresse MongoDB
mongodb://mongo0:27017/ est l'adresse docker par defaut

-v : INFO par defaut, verbosité du logger


Après cette migration, il est possible de lancer des scripts de test pour vérifier que la migration s'est bien réalisée ainsi :

```
pytest -v --input Ichtegem
pytest -v --input Madeleine
pytest -v --input InfoClimat 
```

## Docker image

```
docker build -t migration:latest -f Dockerfile_migration .
```
Pour construire l'image docker des scripts de migration

# EC2

## Upload vers AWS EC2

### Create repo
```
aws ecr create-repository --repository-name migration --region eu-north-1
```

### Tag l'image
```
docker tag migration:latest 333333333333.dkr.ecr.eu-north-1.amazonaws.com/migration:latest
```

Avec *333333333333* ID AWS


### Push l'image
```
docker push 333333333333.dkr.ecr.eu-north-1.amazonaws.com/migration:latest
```

## Log EC2
```
ssh -i C:/Users/Dev/Desktop/AWS/EC2_migration.pem ec2-user@12.34.567.89
```

### Premier lancement d'EC2
```
sudo yum update -y
sudo yum install docker -y
sudo service docker start
sudo usermod -aG docker ec2-user

exit
```

### Optionel : pull l'image de la migration

```
docker pull 333333333333.dkr.ecr.eu-north-1.amazonaws.com/migration:latest
```

### Lance mongo (et la migration)
```
docker-compose build
```

### Acceder la base mongo
```
docker exec -it mongo1 mongosh
```

#### Example de commange mongo
```
use weather_data;
db.weather_station.aggregate([
  {$group: {_id: "$station",
     mean_temperature: { $avg: "$temperature_°C" }}}])
db.weather_station.aggregate([
  {$group: {_id: "$station", 
      max_temperature: { $max: "$temperature_°C" },
      min_temperature: { $min: "$temperature_°C" }}}])
```

### Check migration logs
```
docker logs migration
```

### Pour relancer manuellement les tests
```
docker run -it --rm --network bridge --entrypoint /bin/bash 333333333333.dkr.ecr.eu-north-1.amazonaws.com/migration:latest
```

Lance l'image des scripts sans lancer les scripts eux même
Puis
```
pytest -v --input Ichtegem
pytest -v --input Madeleine
pytest -v --input InfoClimat 
```


### Stop instance ec2
```
aws ec2 stop-instances --instance-ids i-03f7ad7d9d1352c7d
```

### Start instance ec2 (change l'IP)
```
aws ec2 start-instances --instance-ids i-03f7ad7d9d1352c7d
```

puis relancer docker

```
sudo service docker start
```

# Variables d’Environnement

Ce projet utilise plusieurs variables d’environnement pour configurer la connexion MQTT, les informations de l'équilibrage de charge et le système de traitement en parallèle. Voici un aperçu des variables requises, leur rôle et comment les configurer.

## Liste des Variables d’Environnement

1. **`MQTT_BROKER_URI`**  
   - **Rôle** : Définit l'URI du broker MQTT auquel l'application doit se connecter. Cela permet d’établir la connexion entre le système et le broker.
   - **Exemple de valeur** : `tcp://localhost:1883`
   - **Définition** :
     ```bash
     export MQTT_BROKER_URI="tcp://localhost:1883"
     ```

2. **`MQTT_CLIENT_ID`**  
   - **Rôle** : Identifiant unique pour le client MQTT. Il est utilisé pour identifier cette instance spécifique dans le système MQTT.
   - **Exemple de valeur** : `client123`
   - **Définition** :
     ```bash
     export MQTT_CLIENT_ID="client123"
     ```

3. **`MQTT_USERNAME`**  
   - **Rôle** : Nom d’utilisateur pour l’authentification auprès du broker MQTT.
   - **Exemple de valeur** : `username`
   - **Définition** :
     ```bash
     export MQTT_USERNAME="username"
     ```

4. **`MQTT_PASSWORD`**  
   - **Rôle** : Mot de passe pour l’authentification avec le broker MQTT.
   - **Exemple de valeur** : `password`
   - **Définition** :
     ```bash
     export MQTT_PASSWORD="password"
     ```

5. **`OTHER_EDGES`**  
   - **Rôle** : Liste des autres nœuds de bord connectés au réseau pour permettre la communication et le transfert de messages.
   - **Exemple de valeur** : `["tcp://edge1:1883", "tcp://edge2:1883"]` (chaîne JSON)
   - **Définition** :
     ```bash
     export OTHER_EDGES='["tcp://edge1:1883", "tcp://edge2:1883"]'
     ```

6. **Autres paramètres internes**
   - **`loadBalancerIP`** : Défini à `:8081`, adresse d'écoute pour l'équilibrage de charge.
   - **`maxWorkers`** : Défini à `10`, nombre maximum de travailleurs concurrents.
   - **`messageQueueSize`** : Défini à `256`, taille maximale de la file d'attente de messages.

## Comment Configurer les Variables

Pour définir ces variables d'environnement, utilisez la commande `export`, ou ajoutez-les au fichier `.env`. Exemple :

```bash
export MQTT_BROKER_URI="tcp://localhost:1883"
export MQTT_CLIENT_ID="client123"
export MQTT_USERNAME="username"
export MQTT_PASSWORD="password"
export OTHER_EDGES='["tcp://edge1:1883", "tcp://edge2:1883"]'

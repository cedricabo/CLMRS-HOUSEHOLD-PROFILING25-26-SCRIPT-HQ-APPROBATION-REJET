# 📋 CLMRS Household Profiling 25-26 — HQ Validation Script

Ce script automatise le processus de **revue, d'approbation et de rejet** des enquêtes de profilage des ménages (CLMRS) pour la campagne 2025-2026. Il agit comme un filtre de qualité au niveau "Headquarters" (HQ) en interrogeant l'API **Survey Solutions**.

## 🚀 Vue d'Ensemble

Le script analyse les enquêtes soumises, vérifie la cohérence des données, le respect des protocoles (temps, GPS, consentement) et applique des décisions automatiques (Approbation ou Rejet avec commentaires précis pour l'enquêteur).

### Points clés :
 **Technologie :** Python 3.x (API REST, Tenacity pour la résilience réseau).
 **Source de données :** Serveur Survey Solutions (TOUTON).
 **Collaboration :** Logique métier propriétaire avec support de Claude.AI pour les couches de logging et de gestion d'erreurs complexes.

## 🛠️ Critères de Validation (Logique Métier)

Le script rejette automatiquement une enquête si l'un des critères suivants n'est pas rempli :

| Catégorie | Règle de Rejet |
| :--- | :--- |
| **Progression** | Moins de 50 questions activées (enquête incomplète). |
| **Temporel** | Durée totale de l'interview inférieure à 45 minutes. |
| **Géolocalisation** | Coordonnées GPS absentes ou précision > 20 mètres. |
| **Consentement** | Absence de consentement explicite ("Oui" requis). |
| **Intégrité/Fraude** | Contradictions sur le nombre d'enfants ou esquives non justifiées (doublons sans nom). |
| **Formatage** | Saisie en minuscules ou présence d'accents dans les champs de texte libre. |

## 💻 Installation & Configuration

### 1. Prérequis
```bash
pip install requests tenacity openpyxl pandas
```
### 2. Configuration
Les paramètres de connexion se trouvent dans le dictionnaire `CONFIG` au début du script :
* `hq_url` : URL de l'instance Survey Solutions.
* `api_user` & `api_password` : Identifiants avec rôle HQ.
* `questionnaire_id` & `version` : Identifiants techniques du formulaire.

## 📖 Utilisation

Le script propose plusieurs modes d'exécution pour sécuriser le traitement des données :

* **Production :** Traite les enquêtes et applique les décisions sur le serveur.
    ```bash
    python CLMRS_active_script.py
    ```
* **Mode Test (Lecture seule) :** Simule la validation sans modifier les données sur le serveur.
    ```bash
    python CLMRS_active_script.py --test
    ```
* **Mode Diagnostic :** Analyse en profondeur les 5 premières enquêtes (utile pour déboguer les erreurs API).
    ```bash
    python CLMRS_active_script.py --diagnostic
    ```
* **Mode Limité :** Pour traiter un échantillon réduit.
    ```bash
    python CLMRS_active_script.py --limit=20
    ```
* **Réinitialisation :** Efface le fichier de suivi (`checkpoint`) pour retraiter des enquêtes déjà analysées.
    ```bash
    python CLMRS_active_script.py --reset
    ```

## 📊 Sorties et Logs

1.  **`validation_hq.log`** : Journal complet des opérations et des décisions.
2.  **`rapport_validation_hq.xlsx`** : Tableau Excel récapitulatif avec le statut final, le nombre d'enfants et les motifs de rejet.
3.  **`checkpoint_hq.json`** : Fichier de mémoire pour éviter de traiter deux fois la même enquête.

## 📝 Note de l'Auteur
> "Ce script est une base évolutive. Pour les prochaines campagnes, nous prévoyons d'intégrer des scores de performance enquêteur plus poussés et des analyses de cohérence géographique avancées (Géo-fencing). La structure actuelle permet une maintenance facile et une compréhension rapide par de nouveaux utilisateurs."

*Projet : CIV - CLMRS Household Profiling 25-26*

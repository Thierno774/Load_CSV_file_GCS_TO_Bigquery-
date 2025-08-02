 🚀 Load CSV from GCS to BigQuery - Airflow Pipeline

![Airflow DAG Example](docs/images/dag_screenshot.png) *(Optionnel : ajoutez une bannière ou capture principale)*

## 📝 Description
Pipeline Airflow automatisé pour :
- Charger des fichiers CSV/JSON depuis Google Cloud Storage (GCS)
- Créer des tables BigQuery avec validation de schéma
- Générer des vues analytiques
- Gérer les erreurs et logs

## 🛠 Technologies
- **Google Cloud Platform** (GCS, BigQuery)
- **Apache Airflow 2.6+**
- **Python 3.10+**

## 📦 Installation

### Prérequis
```bash
gcloud components install beta
pip install apache-airflow-providers-google==10.0.0

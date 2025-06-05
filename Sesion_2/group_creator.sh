#!/bin/bash

# Configura tus datos
ACCOUNT_ID="TU_ACCOUNT_ID"
TOKEN="TU_ACCOUNT_TOKEN"
GROUP_NAME="data_engineers"
USERS_TO_ADD=("usuario1@empresa.com" "usuario2@empresa.com")

# Endpoint base
API="https://accounts.cloud.databricks.com/api/2.0/accounts/${ACCOUNT_ID}"

# 1. Crear grupo
echo "Creando grupo '${GROUP_NAME}'..."

GROUP_ID=$(curl -s -X POST "${API}/groups" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"display_name\": \"${GROUP_NAME}\"}" | jq -r '.id')

if [[ "$GROUP_ID" == "null" || -z "$GROUP_ID" ]]; then
  echo "Ya existe o error al crear el grupo '${GROUP_NAME}'"
  # Puedes intentar obtener su ID si ya existe
  GROUP_ID=$(curl -s -X GET "${API}/groups" \
    -H "Authorization: Bearer ${TOKEN}" | jq -r ".[] | select(.display_name==\"${GROUP_NAME}\") | .id")
fi

echo "Grupo '${GROUP_NAME}' con ID: ${GROUP_ID}"

# 2. Agregar usuarios
for USER_EMAIL in "${USERS_TO_ADD[@]}"; do
  echo "Agregando usuario '${USER_EMAIL}' al grupo..."

  USER_ID=$(curl -s -X GET "${API}/users" \
    -H "Authorization: Bearer ${TOKEN}" | jq -r ".[] | select(.user_name==\"${USER_EMAIL}\") | .id")

  if [[ -z "$USER_ID" ]]; then
    echo "Usuario '${USER_EMAIL}' no encontrado en la cuenta."
    continue
  fi

  curl -s -X PATCH "${API}/groups/${GROUP_ID}/members" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"members\": [{\"value\": \"${USER_ID}\", \"type\": \"user\"}]}"
  echo "Ysuario agregado: ${USER_EMAIL}"
done

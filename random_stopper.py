import json
import os
import time
import subprocess
import random  # Add import for random

# Archivo JSON con la lista de contenedores
containers_file = "config.json"

# Intervalo en segundos
interval = 10  # Cambia este valor según sea necesario

# Porcentaje de probabilidad de detener un contenedor
stop_probability = 0.3  # 30% de probabilidad

def get_container_names(container_data):
    """Genera los nombres de los contenedores según el formato."""
    container_names = []
    for name, count in container_data.items():
        
        if name == "rabbitmq" or name == "entrypoint":
            continue
        
        if count == 1:
            container_names.append(name)
        else:
            container_names.extend([f"{name}_{i+1}" for i in range(count)])
    return container_names

def stop_and_remove_containers(container_names):
    """Detiene y elimina los contenedores especificados."""
    for container_name in container_names:
        if random.random() < stop_probability:  # Decide si detener el contenedor
            print(f"Attempting to stop container: {container_name}")
            subprocess.run(["docker", "kill", container_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            print(f"Skipping container: {container_name}")

def main():
    # Verifica si existe el archivo JSON
    if not os.path.exists(containers_file):
        print(f"Error: {containers_file} not found.")
        return

    # Lee el archivo JSON
    with open(containers_file, "r") as file:
        container_data = json.load(file)

    # Genera los nombres de los contenedores
    container_names = get_container_names(container_data)

    # Loop infinito con intervalo definido
    while True:
        print(f"Stopping and removing containers: {container_names}")
        stop_and_remove_containers(container_names)
        print(f"Waiting {interval} seconds before the next iteration...")
        time.sleep(interval)

if __name__ == "__main__":
    main()

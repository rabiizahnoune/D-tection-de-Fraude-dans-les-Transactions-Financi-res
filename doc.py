import os

# Chemin du répertoire du projet (par défaut, répertoire courant)
project_path = os.getcwd()
output_file = "project_structure.txt"

# Fonction pour parcourir et écrire la structure et le contenu
def generate_project_structure(root_dir, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"Structure du projet : {root_dir}\n")
        f.write("=" * 50 + "\n\n")
        
        # Parcourir tous les fichiers et dossiers
        for root, dirs, files in os.walk(root_dir):
            # Ignorer certains dossiers (comme .git ou node_modules)
            dirs[:] = [d for d in dirs if d not in [".git", "node_modules", "__pycache__"]]
            
            # Écrire le chemin relatif du dossier courant
            relative_path = os.path.relpath(root, root_dir)
            if relative_path == ".":
                relative_path = "Racine du projet"
            f.write(f"Dossier : {relative_path}\n")
            f.write("-" * 40 + "\n")
            
            # Écrire les fichiers dans ce dossier
            for file_name in files:
                file_path = os.path.join(root, file_name)
                f.write(f"Fichier : {file_name}\n")
                
                # Lire et écrire le contenu du fichier (si c'est un fichier texte)
                try:
                    with open(file_path, "r", encoding="utf-8") as file_content:
                        content = file_content.read()
                        f.write("Contenu :\n")
                        f.write(content + "\n")
                except (UnicodeDecodeError, IOError):
                    f.write("Contenu : [Non lisible - fichier binaire ou encodage incompatible]\n")
                f.write("-" * 40 + "\n")
            f.write("\n")

    print(f"Structure générée dans {output_file}")

# Exécuter le script
if __name__ == "__main__":
    generate_project_structure(project_path, output_file)
require 'json'

Vagrant.configure("2") do |config|

  key_path = "./gcp-key.json"

  file_content = File.read(key_path)
  data = JSON.parse(file_content)

  google_project_id = data["project_id"]
  google_email = data["client_email"]

  bucket_name = "adpd-dados-grupo8-final"
  if ENV['GOOGLE_BUCKET_NAME']
    bucket_name = ENV['GOOGLE_BUCKET_NAME']
  end

  # Configuracao da maquina virtual
  config.vm.box = "google/gce"

  config.vm.provider :google do |google, override|
    google.google_project_id = google_project_id
    google.google_json_key_location = key_path
    google.service_account = google_email

    google.scopes = ['https://www.googleapis.com/auth/cloud-platform']
    google.image_family = 'ubuntu-2404-lts-amd64'
    google.image_project_id = 'ubuntu-os-cloud'
    google.machine_type = 'e2-medium'
    google.zone = 'us-central1-a'
    google.disk_size = 30
    google.preemptible = true
    google.auto_restart = false
    google.on_host_maintenance = "TERMINATE"
    
    ssh_content = File.read("gcp_key.pub").strip
    google.metadata = { 'ssh-keys' => "vagrant:#{ssh_content}" }

    override.ssh.username = "vagrant"
    override.ssh.private_key_path = "/app/gcp_key"
  end

  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provision "file", source: "./src", destination: "$HOME/src"
  config.vm.provision "file", source: "./gcp-key.json", destination: "$HOME/gcp-key.json"
  config.vm.provision "file", source: "./bootstrap.sh", destination: "$HOME/bootstrap.sh"
  config.vm.provision "file", source: "./requirements.txt", destination: "$HOME/requirements.txt"

  config.vm.provision "shell", privileged: false, inline: "chmod +x $HOME/bootstrap.sh && $HOME/bootstrap.sh #{bucket_name}"

  # Destroi a maquina no fim para nao gastar dinheiro
  config.trigger.after :up do |trigger|
    trigger.name = "Auto-Destroy"
    trigger.info = "Processo terminado. A desligar a maquina..."
    trigger.run = { inline: "vagrant destroy -f" }
  end
  
end
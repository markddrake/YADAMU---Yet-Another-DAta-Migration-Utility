#
# Install VIM via Chocolatey
#
Invoke-WebRequest https://chocolatey.org/install.ps1 -UseBasicParsing | Invoke-Expression
choco install -y vim
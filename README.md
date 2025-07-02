## Prerequisites 

> 💡https://www.docker.com/products/docker-desktop/


## Install WSL

```bash

wsl --install 

```

## Then in wsl environment it should be installed


### AWS CLI

```

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

#  If not installed 
sudo apt install unzip -y  

unzip awscliv2.zip
sudo ./aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update


 # Confirm the installation with the following command.

    aws --version

```

### SETUP AWS account

```bash 

aws configure

AWS Access Key ID = <given-access-key>
AWS Secret Access Key = <given-secret-key>
Default region name = <eu-west-1>
Default output format ENTER



```

### SETUP CONDA 

```bash

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

bash ~/Miniconda3-latest-Linux-x86_64.sh
```

Follow the prompts:

- ✅ Accept the license terms
- ✅ Confirm the installation location
- ✅ Choose **"Yes"** when asked to initialize Miniconda in your shell

> 💡 Choosing "Yes" allows Conda to automatically modify your shell configuration so it works in new terminals.

#### 🔄 Apply the changes (or restart terminal)

```bash
source ~/.bashrc

```




### Setup env


#### 🔄 Apply the libraries in linux 

```bash

sudo apt-get update && sudo apt-get install -y tesseract-ocr tesseract-ocr-pol libtesseract-dev libleptonica-dev poppler-utils pkg-config && sudo apt-get clean


```

#### 🔄 Create conda env in linux wsl

```bash
conda env create -f environment.yml
```

#### 💡 Updating conda env in linux wsl

```bash
conda env update -f environment.yml --prune
```


# 📂 **Current code is located in**: `/preprocessing/kkk.py`





## Updating Dask image for cluster

```bash

.\dask\push-to-ecr.ps1

```



## Configure git

``` bash

git config --global user.name xolar1989
git config --global user.email xolar1989@o2.pl
git init 
git remote add origin https://github.com/xolar1989/lawyer_llm_v2.git
git fetch origin



```

## Configure BastionHost for mongodb local

```bash

ssh tunnel to bastion
 ssh -i "BASTION_KEYS.pem" -L 27017:documentdb-cluster.cluster-cywgk7qvlcrn.eu-central-1.docdb.amazonaws.com:27017 ubuntu@ec2-3-69-31-158.eu-central-1.compute.amazonaws.com -N




```



### Create BastionHost

```bash

Variables:
- BastionHostKeyBucketName=bastion-host-key-eu-west-1 



sam package --template-file C:/Users/karol/Desktop/fame_cloud/iaas-cloud/CloudFormation/lambda-bastion-host/bastion-host.yaml --region eu-west-1 --resolve-s3 --output-template-file C:/Users/karol/Desktop/fame_cloud/iaas-cloud/CloudFormation/lambda-bastion-host/bastion-host-packaged.yaml

sam deploy --template-file C:/Users/karol/Desktop/fame_cloud/iaas-cloud/CloudFormation/lambda-bastion-host/bastion-host-packaged.yaml --parameter-overrides BastionHostKeyBucketName=bastion-host-key-eu-west-1 BastionHostLambdaLayerBucketName=bastion-host-lambda-layer-eu-west-1 --stack-name bastion-host-stack --region eu-west-1 --capabilities CAPABILITY_IAM



``` 

### Download key pair for bastion host

```bash
aws s3 cp s3://bastion-host-key-eu-west-1/bastion-key-pair.pem  ./bastion-key-pair.pem 
```




### Get document


https://api.sejm.gov.pl/eli/acts/DU/2020/412/text/T/D20200412L.pdf


I need create api facade for this api



### legal judgements
https://www.saos.org.pl/help/index.php/dokumentacja-api/api-przegladania-danych


### Problem with wsl, (DNS issue)

https://gist.github.com/coltenkrauter/608cfe02319ce60facd76373249b8ca6


### Rozporządzenia 



nie mi chodzi czy mogę użyć rozporządzenia jako podstawy prawnej
ChatGPT powiedział:
ChatGPT
Tak, można użyć rozporządzenia jako podstawy prawnej w sprawie prawnej, ale z pewnymi zastrzeżeniami.

Kluczowe zasady dotyczące rozporządzeń:
Charakter wykonawczy: Rozporządzenia są aktami wykonawczymi do ustaw. Oznacza to, że rozporządzenie nie może stanowić samodzielnej podstawy prawnej – musi być wydane na podstawie ustawy i w celu jej wykonania. Z tego wynika, że:

Rozporządzenie nie może sprzeciwiać się ustawie, a jego przepisy muszą być zgodne z przepisami wyższego rzędu.
Rozporządzenie doprecyzowuje i uszczegóławia przepisy zawarte w ustawie, np. poprzez określenie szczegółowych warunków, procedur czy wymagań technicznych.
Użycie rozporządzenia jako podstawy prawnej:

W praktyce można powołać się na przepisy rozporządzenia jako podstawę prawną do argumentacji, odwołania czy wniosku w sprawie prawnej, pod warunkiem że rozporządzenie to zostało wydane na podstawie odpowiedniej ustawy.
Na przykład, w odwołaniu od decyzji administracyjnej można wskazać, że dana decyzja została wydana z naruszeniem konkretnego przepisu rozporządzenia. Jednak zawsze należy pamiętać, że nadrzędną podstawą prawną jest ustawa, na której opiera się rozporządzenie.
Przykłady użycia rozporządzenia jako podstawy prawnej:
W sprawie dotyczącej np. prawa budowlanego, można powołać się na konkretne przepisy rozporządzenia wykonawczego określającego szczegółowe wymogi techniczne dla budynków.
W sprawach administracyjnych, np. dotyczących zezwoleń, można powołać się na przepisy rozporządzenia, które określają warunki, jakie muszą zostać spełnione, aby uzyskać dane zezwolenie.
Ograniczenia:
Pierwszeństwo ustawy: Jeśli rozporządzenie jest niezgodne z ustawą, można powołać się na przepisy ustawy, wskazując, że rozporządzenie zostało zastosowane niewłaściwie.
Zakres rozporządzenia: Rozporządzenie może działać tylko w zakresie, który jest przewidziany w ustawie. Nie może nakładać nowych obowiązków, które nie wynikają z ustawy.
Podsumowanie:
Możesz użyć rozporządzenia jako podstawy prawnej w swojej sprawie, o ile jest ono zgodne z ustawą i stanowi część szczegółowych przepisów wykonawczych. Rozporządzenie może być skutecznym argumentem, jeśli odnosi się bezpośrednio do twojej sprawy i znajduje zastosowanie w kontekście obowiązujących przepisów ustawowych.

Czy masz konkretny przykład, do którego chciałbyś odnieść się na podstawie rozporządzenia?


## TODO
Ogarnij rozdziały ponieważ mogą zawierać rodziały itd

![img_1.png](img_1.png)


![img.png](img.png)



I am able to connect to the database now.

Make a tunnel through Bastion to the Database from the terminal.

ssh -L 27017:{mongodb-host}:27017 host1

I am able to connect to database via the tunnel from localhost in another terminal.

mongo --host 127.0.0.1:27017 --username {username} --password {password}

So I am able to connect through mongoose too using connection string. mongodb://dbadmin:{username}:{password}@localhost:27017


You may also want to add --sslAllowInvalidHostnames (as described here.) I had to add it due to Hostname: 127.0.0.1 does not match SAN(s)


## Just read this 
https://praveenng.medium.com/how-to-connect-to-a-mongodb-server-in-private-network-using-mongodb-compass-f23f2c89c7d3
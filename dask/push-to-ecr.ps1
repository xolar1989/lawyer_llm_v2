param (
    [string]$RepositoryName="dask-image",
    [string]$AWSAccountID= $(aws sts get-caller-identity --query "Account" --output text),  # Dynamically fetch AWS Account ID
    [string]$AWSRegion = "eu-west-1",
    [string]$ImageTag = "latest",  # Default tag is 'latest' if not provided
    [string]$AWSCredentialsFilePath = "C:\Users\karol\.aws\credentials"  # Path to AWS credentials file
)

function Get-AWSCredentials {
    param([string]$Profile = "default")

    # Read the credentials file and remove any hidden characters
    $rawCredentials = Get-Content -Path $AWSCredentialsFilePath | ForEach-Object { $_.Trim() }

    # Parse the credentials
    $creds = @{}
    $currentProfile = ""

    foreach ($line in $rawCredentials) {
        if ($line -match '^\[(.*?)\]$') {
            $currentProfile = $matches[1]
        } elseif ($line -match '^\s*([^=]+)\s*=\s*(.+)\s*$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            $creds["$currentProfile.$key"] = $value
        }
    }

    # Check if the required profile exists
    if ($creds.ContainsKey("${Profile}.aws_access_key_id") -and $creds.ContainsKey("${Profile}.aws_secret_access_key")) {
        return @{
            AWSAccessKeyID = $creds["${Profile}.aws_access_key_id"]
            AWSSecretAccessKey = $creds["${Profile}.aws_secret_access_key"]
        }
    } else {
        throw "Profile '$Profile' not found in credentials file."
    }
}

# Retrieve AWS credentials from the file
try {
    $awsCreds = Get-AWSCredentials -Profile "default"
    $AWSAccessKeyID = $awsCreds.AWSAccessKeyID
    $AWSSecretAccessKey = $awsCreds.AWSSecretAccessKey
    Write-Host "Successfully retrieved AWS credentials from file."
} catch {
    Write-Host "Failed to retrieve AWS credentials: $($_.Exception.Message)"
    exit 1
}

Write-Host "AWS access key ID: $AWSAccessKeyID"
Write-Host "AWS secret access key: $AWSSecretAccessKey"

function Get-RepositoryExists {
    function Get-RepositoryExists {
        Write-Host "Checking if repository $RepositoryName exists..."
        $result = aws ecr describe-repositories --repository-names $RepositoryName --region $AWSRegion 2>&1

        if ($result -like "*RepositoryNotFoundException*") {
            Write-Host "Repository $RepositoryName does not exist."
            return $false
        } else {
            Write-Host "Repository $RepositoryName exists."
            return $true
        }
    }
}

# Create the repository if it doesn't exist
if (-not (Get-RepositoryExists)) {
    Write-Host "Repository $RepositoryName does not exist. Creating it now..."
    try {
        aws ecr create-repository --repository-name $RepositoryName --region $AWSRegion | Out-Null
        Write-Host "Repository $RepositoryName created successfully."
    } catch {
        Write-Host "Failed to create repository: $($_.Exception.Message)"
        exit 1
    }
} else {
    Write-Host "Repository $RepositoryName already exists."
}






# Get ECR login password
$Password = aws ecr get-login-password --region $AWSRegion

# Login to ECR using the password and pipe it correctly to docker login
$Password | docker login --username AWS --password-stdin "${AWSAccountID}.dkr.ecr.${AWSRegion}.amazonaws.com"
# Build Docker image from Dockerfile
$ImageURI = "${AWSAccountID}.dkr.ecr.${AWSRegion}.amazonaws.com/${RepositoryName}:${ImageTag}"
try {
    docker build --build-arg AWS_ACCESS_KEY_ID=$AWSAccessKeyID `
                 --build-arg AWS_SECRET_ACCESS_KEY=$AWSSecretAccessKey `
                 --build-arg AWS_REGION=$AWSRegion `
                 -t $ImageURI -f Dockerfile ../
    Write-Host "Docker image built successfully."
} catch {
    Write-Host "Failed to build Docker image: $($_.Exception.Message)"
    exit 1
}
# Tag Docker image
docker tag $ImageURI "${AWSAccountID}.dkr.ecr.${AWSRegion}.amazonaws.com/${RepositoryName}:${ImageTag}"

# Push Docker image to ECR
docker push "${AWSAccountID}.dkr.ecr.${AWSRegion}.amazonaws.com/${RepositoryName}:${ImageTag}"

# Output success message
Write-Host "Docker image pushed successfully to ECR: $ImageURI"
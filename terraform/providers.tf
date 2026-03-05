terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "personal-incomes-terraform-state"
    key            = "finance-query-agent/state.tfstate"
    region         = "us-east-1"
    dynamodb_table = "personal-incomes-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}

provider "aws" {
    region = "us-east-2"
}

resource "aws_s3_bucket" "data_lake" {
    bucket = "aws-healthcare-data-lake"

    tags={
        project = "aws-healthcare-data-lake"
        Environment = "dev"
        ManagedBy = "terraform"
    }
}

resource "aws_s3_object" "bronze" {
    bucket = aws_s3_bucket.data_lake.bucket
    key = "bronze/"
}

resource "aws_s3_object" "silver" {
    bucket = aws_s3_bucket.data_lake.bucket
    key = "silver/"
}

resource "aws_s3_object" "gold" {
    bucket = aws_s3_bucket.data_lake.bucket
    key = "gold/"
}

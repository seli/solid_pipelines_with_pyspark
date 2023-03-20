resource "aws_secretsmanager_secret" "api_key" {
  name = "some_config_value"
  description = "API key for the OpenWeatherMap service"
}


resource "aws_secretsmanager_secret_version" "api_value" {
  secret_id     = aws_secretsmanager_secret.api_key.id
  secret_string = "garbage-example-${random_pet.rnd.id}"
}

resource "random_pet" "rnd" {
  length = 2
}
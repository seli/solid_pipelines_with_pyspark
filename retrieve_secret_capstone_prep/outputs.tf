output "iam_access_key" {
  value = aws_iam_access_key.participant.id
}

output "pgp_encrypted_iam_secret_access_key" {
  value = aws_iam_access_key.participant.encrypted_secret
  # Decrypt using your private PGP key:
  # terraform output -raw pgp_encrypted_iam_secret_access_key| base64 --decode | keybase pgp decrypt
}

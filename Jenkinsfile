pipeline {
    agent any

    stages {

        stage('Repo Cloned Check') {
            steps {
                echo 'Jenkins has cloned the repository successfully.'
            }
        }

        stage('List Repository Files') {
            steps {
                echo 'Listing repository contents...'
                bat 'dir'
            }
        }

        stage('Validate Folder Structure') {
            steps {
                echo 'Checking required folders...'
                bat '''
                if exist glue_jobs (
                    echo glue_jobs folder exists
                ) else (
                    echo glue_jobs folder is missing
                    exit /b 1
                )

                if exist .github (
                    echo .github folder exists
                ) else (
                    echo .github folder is missing
                    exit /b 1
                )
                '''
            }
        }
    }

    post {
        success {
            echo 'CI validation completed safely. No files were executed.'
        }
        failure {
            echo 'Repo structure validation failed.'
        }
    }
}

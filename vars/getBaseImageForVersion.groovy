#!groovy
import java.util.regex.Pattern

def call(String version) {
    def pattern = ~/https:.*scylla/ + version.replace(version.length, [\.:'-']) + /[\w\d_-]*/
    def result = sh (returnStdout: true,
                         script: """ ./docker/env/hydra.sh list-gce-images-versions """)
        printf('Docker list-gce-images-versions:\n%s', result)
        match = result =~ pattern
        printf('Matched result: $s', match.findAll()*.first())
    }

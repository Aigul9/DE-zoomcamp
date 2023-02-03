from prefect.filesystems import GitHub

block = GitHub(
    repository='https://github.com/Aigul9/DE-zoomcamp/'
)

block.save('dev')  # block name

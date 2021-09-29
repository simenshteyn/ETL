# Deployment instructions

1. Setup config via variables in `.env` file:\
``$ vi .env``
2. First run with auto data migration (only once):\
`$ docker-compose --profile init up --build -d`
3. Further runs django without data migration:\
`$ docker-compose up -d`
4. Login at site: [http://localhost/admin/](http://localhost/admin/)
5. Explore API at site: [http://localhost/api/v1/movies/](http://localhost/api/v1/movies/)
6. To stop app run:\
`$ docker-compose stop`
7. To start app again run:\
`$ docker-compose start`
8. To restart app run:\
`$ docker-compose restart`
9. To remove app with all data run :\
`$ docker-compose down -v`
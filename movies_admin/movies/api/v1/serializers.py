from rest_framework import serializers
from rest_framework.serializers import ModelSerializer

from movies.models import Movies, Genres


class GenresSerializer(serializers.RelatedField):
    def to_representation(self, value):
        return value.genre_name

    class Meta:
        model = Genres
        ordering = ['-id']


class PeopleSerializer(serializers.RelatedField):
    def to_representation(self, value):
        return value.person.full_name


class MoviesSerializer(ModelSerializer):
    id = serializers.UUIDField(source='movie_id')
    title = serializers.CharField(source='movie_title')
    description = serializers.CharField(source='movie_desc')
    type = serializers.CharField(source='movie_type')
    creation_date = serializers.DateTimeField(format='%Y-%m-%d',
                                              source='created_at')
    rating = serializers.FloatField(source='movie_rating')
    genres = GenresSerializer(source='movie_genres',
                              read_only=True, many=True)
    actors = PeopleSerializer(source='actors_field',
                              many=True, read_only=True)
    directors = PeopleSerializer(source='directors_field',
                                 many=True, read_only=True)
    writers = PeopleSerializer(source='writers_field',
                               many=True, read_only=True)

    class Meta:
        model = Movies
        ordering = ['-id']
        fields = ['id',
                  'title',
                  'description',
                  'creation_date',
                  'rating',
                  'type',
                  'genres',
                  'actors',
                  'directors',
                  'writers'
                  ]
        read_only_fields = fields

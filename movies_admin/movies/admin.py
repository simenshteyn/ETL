from django.contrib import admin

from .models import Movies, People, Genres, MoviePeople, MovieGenres


class MovieGenresInline(admin.TabularInline):
    model = MovieGenres
    extra = 0
    show_change_link = True

    def get_queryset(self, request):
        queryset = super(MovieGenresInline, self).get_queryset(request)
        queryset = queryset.select_related('movie', 'genre')
        return queryset


class MoviePeopleInline(admin.TabularInline):
    model = MoviePeople
    extra = 0
    show_change_link = True
    autocomplete_fields = ('person', 'movie')

    def get_queryset(self, request):
        queryset = super(MoviePeopleInline, self).get_queryset(request)
        queryset = queryset.select_related('movie', 'person')
        return queryset


@admin.register(Genres)
class GenresAdmin(admin.ModelAdmin):
    search_fields = ['genre_name']
    ordering = ['genre_name']
    list_display = ['genre_name']


@admin.register(People)
class PeopleAdmin(admin.ModelAdmin):
    search_fields = ['full_name']
    list_display = ['full_name']
    ordering = ['full_name']
    inlines = [MoviePeopleInline]


@admin.register(Movies)
class MoviesAdmin(admin.ModelAdmin):
    list_display = ['movie_title', 'movie_rating', 'genres']
    search_fields = ['movie_title']
    ordering = ['movie_title', 'movie_rating']
    inlines = [MovieGenresInline, MoviePeopleInline]

    def get_queryset(self, request):
        queryset = super(MoviesAdmin, self).get_queryset(request)
        queryset = queryset.prefetch_related('movie_genres')
        return queryset

    def genres(self, instance):
        result = ", ".join([
            genre.genre_name for genre in instance.movie_genres.all()
        ])
        return result

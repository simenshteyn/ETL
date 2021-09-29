from django.db.models import Prefetch
from rest_framework import pagination
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from movies.models import Movies, MoviePeople, People
from movies.api.v1.serializers import MoviesSerializer


class CustomPagination(pagination.PageNumberPagination):
    def get_paginated_response(self, data):
        return Response({
            'count': self.page.paginator.count,
            'total_pages': self.page.paginator.num_pages,
            'prev': self.page.previous_page_number() if self.page.number > 1
            else None,
            'next': self.page.next_page_number()
            if self.page.number < self.page.paginator.num_pages else None,
            'result': data,
        })


class MoviesViewSet(ReadOnlyModelViewSet):
    queryset = Movies.objects.prefetch_related(
        Prefetch('movie_genres'),
        Prefetch('people_related', queryset=MoviePeople.objects
                 .filter(person_role='actor').distinct('movie_id')
                 .select_related('person'),
                 to_attr='actors_field'),
        Prefetch('people_related', queryset=MoviePeople.objects
                 .filter(person_role='writer').distinct('movie_id')
                 .select_related('person'),
                 to_attr='writers_field'),
        Prefetch('people_related', queryset=MoviePeople.objects
                 .filter(person_role='director').distinct('movie_id')
                 .select_related('person'),
                 to_attr='directors_field'),
    )
    serializer_class = MoviesSerializer
    pagination_class = CustomPagination

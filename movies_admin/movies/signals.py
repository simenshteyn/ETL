import datetime

from movies.models import People

from django.db.models.signals import post_save

print('signals imported')


def congratulatory(sender, instance, created, **kwargs):
    if created and instance.birthday == datetime.date.today():
        print(f"У {instance.full_name} сегодня день рождения! 🥳")


post_save.connect(receiver=congratulatory,
                  sender=People, weak=True,
                  dispatch_uid='congratulatory_signal')

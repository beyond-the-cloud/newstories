# newstories
Get the new 500 stories through [Hacker News API](https://github.com/HackerNews/API#new-top-and-best-stories)

```bash
docker build -t newstories:1.0 .

docker run -it newstories:1.0

docker tag newstories:1.0 bh7cw/newstories:1.0
docker push bh7cw/newstories:1.0
```
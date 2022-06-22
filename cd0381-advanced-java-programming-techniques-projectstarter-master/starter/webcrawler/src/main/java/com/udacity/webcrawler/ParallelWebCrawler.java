package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final PageParserFactory parserFactory;
  private final List<Pattern> ignoredUrls;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls
          ) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.parserFactory = parserFactory;
    this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);

    Map<String, Integer> counts = new ConcurrentHashMap<>() ;
    Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

    for (String url : startingUrls) {
      pool.invoke(new WebCrawlerAction(url, counts, visitedUrls, maxDepth, deadline));
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  public final class WebCrawlerAction extends RecursiveAction {
    private final String url;
    private final Map<String, Integer> counts;
    Set<String> visitedUrls;
    private final int maxDepth;
    private final Instant deadline;

    public WebCrawlerAction(String url,
                          Map<String, Integer> counts,
                          Set<String> visitedUrls,
                          int maxDepth,
                          Instant deadline) {
      this.url = url;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
      this.maxDepth = maxDepth;
      this.deadline = deadline;
    }

    @Override
    protected void compute() {

      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return;
      }

      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return;
        }
      }

      if (!visitedUrls.add(url)) {
        return;
      }

      PageParser.Result result = parserFactory.get(url).parse();

      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        if (counts.containsKey(e.getKey())) {
          counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
        } else {
          counts.put(e.getKey(), e.getValue());
        }
      }

      List<WebCrawlerAction> subtasks = new ArrayList<>();

      for (String link : result.getLinks()) {
        subtasks.add(new WebCrawlerAction(link, counts, visitedUrls, maxDepth - 1, deadline));
      }
      invokeAll(subtasks);
    }
  }
}
package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final PageParserFactory parserFactory;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.parserFactory = parserFactory;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadTime = clock.instant().plus(timeout);
    Map<String,Integer> counts = Collections.synchronizedMap(new HashMap<>());
    Set<String> visitedUrls = Collections.synchronizedSet(new HashSet<>());
    for (String url:startingUrls){
      pool.invoke(new CrawlInternal(url, deadTime, maxDepth, counts, visitedUrls));
    }
    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }
    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();

    //return new CrawlResult.Builder().build();
  }
  public class CrawlInternal extends RecursiveAction{
    private String url;
    private Instant deadtime;
    private int maxDepth;
    private Map<String,Integer> counts;
    private Set<String> visitedUrls;

    public CrawlInternal(String url,Instant deadtime,int maxDepth,Map<String,Integer>  counts,Set<String> visitedUrls){
      this.url = url;
      this.deadtime = deadtime;
      this.counts = counts;
      this.maxDepth = maxDepth;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected void compute(){
      if (maxDepth == 0 || clock.instant().isAfter(deadtime)){
        return;
      }
      for (Pattern pattern:ignoredUrls){
        if (pattern.matcher(url).matches()){
          return;
        }
      }
      if (visitedUrls.contains(url)){
        return;
      }
      visitedUrls.add(url);

      PageParser.Result result = parserFactory.get(url).parse();
      for (Map.Entry<String,Integer> e : result.getWordCounts().entrySet()){
        if (counts.containsKey(e.getKey())){
          counts.put(e.getKey(),e.getValue()+counts.get(e.getKey()));
        }else {
          counts.put(e.getKey(),e.getValue());
        }
      }
      List<CrawlInternal> subTasks = new ArrayList<>();
      for (String link: result.getLinks()){
        subTasks.add(new CrawlInternal(link,deadtime,maxDepth-1,counts,visitedUrls));
      }
      invokeAll(subTasks);

    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}

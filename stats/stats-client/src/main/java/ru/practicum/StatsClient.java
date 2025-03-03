package ru.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import org.springframework.http.HttpStatus;
import ru.practicum.exception.StatsBadRequestException;

import java.net.URI;
import java.util.List;

@Component
@Slf4j
public class StatsClient {

    private final WebClient webClient;
    private final DiscoveryClient discoveryClient;
    private final RetryTemplate retryTemplate;
    private final String statsServiceId;  // Имя сервиса в Eureka

    public StatsClient(WebClient.Builder webClientBuilder,
                       DiscoveryClient discoveryClient,
                       @Value("${stats.service.id}") String statsServiceId) {
        this.webClient = webClientBuilder.build();  // Базовый URL будет динамически определяться через Eureka
        this.discoveryClient = discoveryClient;
        this.statsServiceId = statsServiceId;
        this.retryTemplate = createRetryTemplate();
    }

    /**
     * Сохраняет информацию о запросе в stats-service.
     *
     * @param hit информация о запросе
     */
    public void saveHit(EndPointHitDto hit) {
        retryTemplate.execute(context -> {
            URI uri = makeUri("/hit");
            log.debug("Sending hit to stats-service: {}", uri);
            webClient.post()
                    .uri(uri)
                    .bodyValue(hit)
                    .retrieve()
                    .bodyToMono(Void.class)
                    .block();
            return null;
        });
    }

    /**
     * Получает статистику из stats-service.
     *
     * @param start  начало периода
     * @param end    конец периода
     * @param uris   список URI
     * @param unique учитывать только уникальные запросы
     * @return массив статистики
     */
    public ViewStatsDto[] getStats(String start, String end, String[] uris, boolean unique) {
        return retryTemplate.execute(context -> {
            URI uri = makeUri("/stats");
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUri(uri)
                    .queryParam("start", start)
                    .queryParam("end", end)
                    .queryParam("unique", unique);

            for (String uriParam : uris) {
                uriBuilder.queryParam("uris", uriParam);
            }

            String fullUri = uriBuilder.build().toUriString();
            log.debug("Requesting stats from stats-service: {}", fullUri);

            try {
                return webClient.get()
                        .uri(fullUri)
                        .retrieve()
                        .onStatus(
                                status -> status.is4xxClientError() || status.is5xxServerError(),
                                clientResponse -> {
                                    if (clientResponse.statusCode().equals(HttpStatus.BAD_REQUEST)) {
                                        return clientResponse.bodyToMono(String.class)
                                                .flatMap(body -> Mono.error(new StatsBadRequestException("Bad Request: " + body)));
                                    } else {
                                        return clientResponse.bodyToMono(String.class)
                                                .flatMap(body -> Mono.error(new RuntimeException("Request failed: " + body)));
                                    }
                                })
                        .bodyToMono(ViewStatsDto[].class)
                        .block();
            } catch (StatsBadRequestException e) {
                log.error("Bad Request Exception: {}", e.getMessage());
                return new ViewStatsDto[0];  // Fallback: возвращаем пустой массив
            } catch (Exception e) {
                log.error("Error during request: {}", e.getMessage());
                return new ViewStatsDto[0];  // Fallback: возвращаем пустой массив
            }
        });
    }

    /**
     * Создает RetryTemplate для повторных попыток.
     *
     * @return RetryTemplate
     */
    private RetryTemplate createRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(3000L);  // Пауза между попытками (3 секунды)
        retryTemplate.setBackOffPolicy(backOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);  // Максимальное количество попыток
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }

    /**
     * Получает URI для вызова stats-service через Eureka.
     *
     * @param path путь к API
     * @return полный URI
     */
    private URI makeUri(String path) {
        List<ServiceInstance> instances = discoveryClient.getInstances(statsServiceId);
        if (instances.isEmpty()) {
            throw new RuntimeException("Сервис статистики не найден: " + statsServiceId);
        }
        ServiceInstance instance = instances.getFirst();
        return URI.create("http://" + instance.getHost() + ":" + instance.getPort() + path);
    }
}
# python-kafka

### Counsumer 주요 옵션
bootstrap.servers
정의된 포맷: kafka01:9092, kafka02:9092….
전체 카프카 리스트를 적어주는 것을 권장. 하나만 사용시 장애 발생시 불능

fetch.min.byte
한번에 가져올 수 있는 최소 데이터 사이즈
지정한 크기보다 작다면 데이터가 누적될 때까지 기다림

group.id
컨슈머가 속한 컨슈머 그룹을 식별하는 식별자

enable.auto.commit
백그라운드로 주기적으로 오프셋을 커밋함
enable.auto.commit=true 로 설정하면 5초마다 컨슈머는 poll()를 호출할 때 가장 마지막 오프셋을 커밋함(자동 커밋).
5초 주기는 기본값이며, auto.commit.interval.ms 옵션을 통해 조정 가능.

auto.offset.reset
오프셋이 없는 경우(데이터가 없음)에 아래의 옵션으로 리셋함
earliest : 가장 초기의 오프셋 값으로 설정
latest : 가장 마지막의 오프셋 값으로 설정
none : 이전 오프셋값을 찾지 못하면 에러를 나타냄

fetch.max.bytes
한번에 가져올 수 있는 최대 데이터 사이즈

request.timeout.ms
요청에 대해 응답을 기다리는 최대 시간

session.timeout.ms
컨슈머가 살아있는 것으로 판단하는 시간(디폴트 10초)
10초 안에 컨슈머가 그룹 코디네이터에게 하트비트를 보내야함
10초를 넘으면 장애로 판단하고 리밸런스를 시도함
세션 타임아웃이 짧다면 가비지 컬렉션이나 poll loop 완료시간이 길어지게 되어 리밸런스가 일어나기도 함.
세션 타임아웃이 길다면 리밸런스보다 실제 오류 탐시 시간이 오래 걸림

heartbeat.interval.ms
그룹 코디네이터에게 얼마나 자주 KafkaConsumer poll() 메소드로 하트비트를 보낼지 조정함.
session.timeout.ms과 밀접하며 반드시 session.timeout.ms의 값보다 당연히 작아야 하며 일반적으로 1/3로 설정. 디폴트는 3초

max.poll.records
단일 호출 poll()에 대한 최대 레코드 수를 조정.

max.poll.interval.ms
컨슈머가 하트비트를 주기적으로 보내는데, 하트비트만 보내고 메시지를 가져가지 않은 경우, 무기한 파티션 점유를 막기 위함
즉 컨슈머가 poll()을 호출하지 않으면 장애로 판단하며, 다른 컨슈머에게 메시지를 가져가도록 함.

auto.commit.interval.ms
주기적으로 오프셋을 커밋하는 시간

fetch.max.wait.ms
fetch.min.bytes의 설정 데이터보다 적은 경우 요청에 응답을 기다리는 최대 시간.
fetch.min.bytes는 한번에 가져가는 최대 데이터 사이즈인데 무한정 데이터가 차길 기다릴 수 없으니 시간제약을 준다고 생각하자.

[참고]
- https://github.com/dpkp/kafka-python
- https://kafka-python.readthedocs.io/en/master/usage.html

<html>
<head>
	<!-- 이 페이지를 열어둔 상태로 59.5 초 마다 확인 하게끔 설정 -->
	<META HTTP-EQUIV="refresh" CONTENT="59.5">
	<meta charset="utf-8">
</head>
<body>
<?
// Snoopy.class.php 사용하게끔 포함 하기.
include_once './insert/Snoopy.class.php';
$snoopy = new snoopy;
// 비트렉스의 값으로 불러오기.
$snoopy->fetch("https://bittrex.com/api/v1.1/public/getmarketsummaries");
$t	 = $snoopy->results;

// Snoopy 링크 참고
preg_match_all("|MarketName\":\"(.*)\",\"High\"|U", $t, $name, PREG_SET_ORDER);
preg_match_all("|Last\":(.*),\"BaseVolume\"|U", $t, $price, PREG_SET_ORDER);

// 텔레그램 봇 API 값
$api_code = '1867497311:AAHrvdQv-k2s7RoMmJNkJo875LSxLMnGjog';

$int = 0;
WHILE($int < count($name)){
	//XLM 의 값이 0.00003487 미만 이면 알람 받기.
	if($name[$int][1] == 'BTC-XLM' && $price[$int][1] < 0.00003487){
		//받을 메시지 입력
		$telegram_text = $name[$int][1]." 현재 ".$price[$int][1]." 사토시" ;
		//챗 아이디와 메시지 배열에 입력
		$query_array = array(
			'chat_id' => '-1001174905540',
			'text' => $telegram_text,
		);
		// 메시지 보낼 URL
		$request_url = "https://api.telegram.org/bot{$api_code}/sendMessage?" . http_build_query($query_array);
		$curl_opt = array(
				CURLOPT_RETURNTRANSFER => 1,
				CURLOPT_URL => $request_url,
			);
		// curl로 접속
		$curl = curl_init();
		curl_setopt_array($curl, $curl_opt);

		// 응답결과는 알아서 처리.
		var_dump(curl_exec($curl));

	}
	$int++;
}

?>
</body>
</html>

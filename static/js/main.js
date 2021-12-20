$(document).ready(function () {
    var availableName = [
      "Abraham",
      "Elisabeth",
      "Haim",
      "Micky",
      "Miki",
      "Nazim",
      "Rami",
      "Reuven",

    ];
    $("#nameList").autocomplete({
        source: availableName
    });

//    $("#nameList").on("keydown", function (e) { 
//        if (e.keyCode === 13) {
//            serchName();
//        }
//    });

    $("#searchName").on("click", function () {
        serchName();
    })

    $("#nameList").autocomplete({
        autoFocus: true,
        select: function (event, ui) {
            let checkval = ui.item.value
            serchName(checkval);
        }
    });




    function serchName(checkval) { 
 
        if ($(".result-wrapper ul").hasClass("slick-initialized")) {
            $('.result-wrapper ul').slick('unslick');
        }
        
        
        var searchVal;

        if (checkval) {
            searchVal = checkval;
        } else {
            searchVal = $("#nameList").val();
        }
        
        $.getJSON("nameList?name=" + searchVal, function (data) {
            if (typeof data.soundex !== 'undefined'){
                let spoken_name_2_vec = data.spoken_name_2_vec;
            let double_metaphone = data.double_metaphone;
            let family_trees = data.family_trees;
            let match_rating_codex = data.match_rating_codex;
            let metaphone = data.metaphone;
            let nysiis = data.nysiis;
            let soundex = data.soundex;
            let name = data.name

            $("#searchResultList").html(
                `<li id='spokend-name'>
                    <h3>SpokenName2vec : ${name}</h3>
                 </li>

                <li id='double-metaphone'>
                    <h3>Double Metaphone : ${name}</h3>
                 </li>

                <li id='family-trees'>
                    <h3>Family trees: ${name}</h3>
                 </li>
 
                <li id='match-rating-codex'>
                    <h3>Match Rating Codex: ${name}</h3>
                 </li>

                <li id='meta-phone'>
                    <h3>Metaphone : ${name}</h3>
                 </li>

                <li id='nysiis'>
                    <h3>NYSIIS: ${name}</h3>
                 </li>

                <li id='soundex'>
                    <h3>Soundex: ${name}</h3>
                 </li>
                `
            )



            spoken_name_2_vec.forEach(function (data) {
                $("#spokend-name").append(`<span>${data.candidate}<span>`)
            })

            double_metaphone.forEach(function (data) {
                $("#double-metaphone").append(`<span>${data.candidate}<span>`)
            })

            family_trees.forEach(function (data) {
                $("#family-trees").append(`<span>${data.candidate}<span>`)
            })

            match_rating_codex.forEach(function (data) {
                $("#match-rating-codex").append(`<span>${data.candidate}<span>`)
            })

            metaphone.forEach(function (data) {
                $("#meta-phone").append(`<span>${data.candidate}<span>`)
            })

            nysiis.forEach(function (data) {
                $("#nysiis").append(`<span>${data.candidate}<span>`)
            })

            soundex.forEach(function (data) {
                $("#soundex").append(`<span>${data.candidate}<span>`)
            })






            $('.result-wrapper ul').slick({
                dots: false,
                infinite: false,
                speed: 300,
                slidesToShow: 5,
                slidesToScroll: 1,
                nextArrow: '<img class="next" src="static/images/arrow-right.png">',
                prevArrow: '<img class="prev" src="static/images/arrow-left.png">',
                responsive: [

                    {
                        breakpoint: 600,
                        settings: {
                            slidesToShow: 2,
                            slidesToScroll: 2
                        }
    },

  ]
            });
            }

            else{
               $("#searchResultList").html(
                `<h3 style="color: #fff;">No Synonyms Suggested</h3>`

                 )
            }

        });

    }

})

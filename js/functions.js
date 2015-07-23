var followTo = function (selector) {
    var $this = $(selector),
        $window = $(window);

    var sectionHeight = $this.parent().find('section').height();
    var pos = 0;
    if (sectionHeight > $this.height()) {
        pos = sectionHeight - $this.height();
        console.log(sectionHeight, $this.height(), pos);
    }

    $window.scroll(function (e) {
        if ($window.scrollTop() > pos) {
            $this.css({
                position: 'absolute',
                top: pos
            });
        } else {
            $this.css({
                position: 'fixed',
                top: 'auto'
            });
        }
    });
};